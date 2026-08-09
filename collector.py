"""
Фоновый сборщик данных аукциона STALZONE.

Два job'а:
- job_history()   — дополнение auction_history.sqlite3 (режим topup)
- job_active_lots() — атомарный снимок активных лотов в auction_active.sqlite3

После каждого успешного снимка активных лотов collector запускает brain.py
(через прямой импорт и фоновый поток) для немедленного пересчёта fair value
и score — без ожидания таймера опроса.

Запуск:
    python collector.py              # daemon: оба job по расписанию
    python collector.py --once       # один цикл обоих job и выход
    python collector.py --once history
    python collector.py --once active
"""

from __future__ import annotations

import argparse
import json
import logging
import queue
import signal
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import CLIENT_ID, CLIENT_SECRET

import brain as brain_module

sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)  # type: ignore[attr-defined][cite: 6]

# ---- Конфиг ----

BASE_DIR = Path(__file__).resolve().parent

HISTORY_INTERVAL_MIN = 10
ACTIVE_INTERVAL_SEC = 60  # Изменено на 60 секунд (1 минута)
HISTORY_DB = BASE_DIR / "auction_history.sqlite3"
ACTIVE_DB = BASE_DIR / "auction_active.sqlite3"
ITEMS_JSON = BASE_DIR / "items.json"
LOG_FILE = BASE_DIR / "collector.log"

REGION = "RU"
BASE_URL = "https://eapi.stalzone.com"

LIMIT = 200
CUTOFF_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
MAX_WORKERS = 8           # Для Истории
ACTIVE_MAX_WORKERS = 6    # Оптимально для 1-минутного опроса Активных лотов
MIN_REQUEST_INTERVAL = 0.3
MAX_RETRIES = 5
MAX_RETRIES_429 = 8

SORT = "buyout_price"
ORDER = "asc"

KNOWN_ADDITIONAL_FIELDS = frozenset({
    "qlt", "ptn", "stats_random", "upgrade_bonus", "spawn_time", "bonus_properties",
    "it_transf_count", "ndmg", "md_k",
})

# ---- Logging ----

log = logging.getLogger("collector")


def setup_logging() -> None:
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)


# ---- Общая инфраструктура ----

_rate_lock = threading.Lock()
_last_request_time = 0.0
_history_lock = threading.Lock()
_active_lock = threading.Lock()
_shutdown = threading.Event()


def rate_limited_wait() -> None:
    global _last_request_time
    with _rate_lock:
        now = time.monotonic()
        wait = _last_request_time + MIN_REQUEST_INTERVAL - now
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.monotonic()


def headers() -> dict:
    return {"Client-Id": CLIENT_ID, "Client-Secret": CLIENT_SECRET}


def load_item_ids() -> list[str]:
    if not ITEMS_JSON.exists():
        raise SystemExit(
            f"Не найден {ITEMS_JSON}. Сначала запусти update_items.py."
        )
    items = json.loads(ITEMS_JSON.read_text(encoding="utf-8"))
    return list(items.keys())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def api_get_json(url: str, params: dict, item_id: str, use_rate_limit: bool = True) -> dict:
    """GET с rate limit, retry на 429 и сетевые ошибки.[cite: 6]"""
    attempt_429 = 0
    attempt_other = 0
    while True:
        if _shutdown.is_set():
            raise InterruptedError("shutdown")
            
        if use_rate_limit:
            rate_limited_wait()
            
        try:
            resp = requests.get(url, headers=headers(), params=params, timeout=15)
            if resp.status_code == 429:
                attempt_429 += 1
                if attempt_429 > MAX_RETRIES_429:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else min(60, 2 ** attempt_429)
                log.warning("[%s] 429, жду %.1fс (%d/%d)", item_id, wait, attempt_429, MAX_RETRIES_429)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            try:
                return resp.json()
            except (json.JSONDecodeError, ValueError) as e:
                raise requests.RequestException(f"невалидный JSON: {e}") from e
        except requests.RequestException as e:
            attempt_other += 1
            if attempt_other >= MAX_RETRIES:
                raise type(e)(f"[{item_id}] после {MAX_RETRIES} попыток: {e}") from e
            time.sleep(1.5 * attempt_other)


def set_collector_status(status_text: str) -> None:
    """Записывает текущий статус парсера в БД для вывода на сайт."""
    try:
        conn = sqlite3.connect(ACTIVE_DB, timeout=10)
        conn.execute("CREATE TABLE IF NOT EXISTS collector_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute(
            "INSERT OR REPLACE INTO collector_state (key, value) VALUES ('active_job_status', ?)",
            (status_text,)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("Не удалось записать статус в БД: %s", e)


# ---- Brain trigger ----

_brain_lock = threading.Lock()
_brain_inited = False


def notify_server() -> None:
    """Уведомляет сервер (server.py), что активные лоты обновлены.
    Сервер рассылает SSE-событие всем открытым вкладкам, чтобы сайт
    обновил данные без опроса. Если сервер не запущен — молча пропускаем."""
    try:
        requests.post("http://127.0.0.1:8000/api/notify", timeout=2)
    except Exception:
        pass  # сервер может быть не запущен — это не критично


def trigger_brain_recompute() -> None:
    """Запускает brain.recompute_all() в фоновом потоке — полный пересчёт,
    используется как страховка (например после --once) или вручную.
    Основной путь обновления теперь — mark_dirty() + brain_worker_loop
    (см. ниже), они пересчитывают только изменившиеся айтемы сразу по мере
    готовности, не дожидаясь конца всего цикла опроса."""
    global _brain_inited
    if not _brain_inited:
        brain_module.setup_logging()
        _brain_inited = True

    if not _brain_lock.acquire(blocking=False):
        log.info("brain: предыдущий пересчёт ещё идёт, пропуск")
        return

    def _run() -> None:
        try:
            brain_module.recompute_all()
        except Exception:
            log.exception("Ошибка в brain recompute")
        finally:
            _brain_lock.release()

    threading.Thread(target=_run, daemon=True, name="brain-recompute").start()


# ===========================================================================
# Инкрементальный пересчёт мозга: "получили лоты по айтему — сразу посчитали"
# ===========================================================================
# Вместо ожидания конца ВСЕГО цикла опроса (может занимать минуты для ~100
# артефактов), каждый айтем, как только его лоты записаны в БД, помечается
# "грязным". Отдельный фоновый поток с небольшим дебаунсом (DIRTY_DEBOUNCE_SEC)
# собирает пачку айтемов, готовых почти одновременно (параллельные воркеры),
# и просит brain.py пересчитать только их — без глобального DELETE/пересчёта.

DIRTY_DEBOUNCE_SEC = 1.5  # пауза сборки пачки после первого "грязного" айтема

_dirty_items: set[str] = set()
_dirty_lock = threading.Lock()
_dirty_event = threading.Event()


def mark_dirty(item_id: str) -> None:
    """Помечает айтем как требующий пересчёта мозгом и будит фоновый воркер."""
    with _dirty_lock:
        _dirty_items.add(item_id)
    _dirty_event.set()


def brain_worker_loop() -> None:
    """Фоновый поток: ждёт появления грязных айтемов, даёт короткую паузу на
    сборку пачки (пока параллельные воркеры допишут свои результаты), затем
    просит brain.py пересчитать только эту пачку. Если пересчёт не удался
    (например, лок занят полным пересчётом в другом процессе) — айтемы
    возвращаются в очередь и будут подхвачены следующим циклом."""
    global _brain_inited
    if not _brain_inited:
        brain_module.setup_logging()
        _brain_inited = True

    log.info("brain_worker_loop запущен (дебаунс %.1fс)", DIRTY_DEBOUNCE_SEC)
    while not _shutdown.is_set():
        triggered = _dirty_event.wait(timeout=1.0)
        if not triggered:
            continue
        time.sleep(DIRTY_DEBOUNCE_SEC)
        _dirty_event.clear()

        with _dirty_lock:
            batch = list(_dirty_items)
            _dirty_items.clear()
        if not batch:
            continue

        try:
            ok = brain_module.recompute_for_items(batch)
        except Exception:
            log.exception("Ошибка в частичном brain recompute (%d айтемов)", len(batch))
            ok = False

        if ok:
            log.info("brain: частичный пересчёт готов (%d айтемов)", len(batch))
            notify_server()
        else:
            # лок был занят — не потеряли айтемы, вернули в очередь на следующий раунд
            log.info("brain: частичный пересчёт отложен (%d айтемов), возвращаю в очередь", len(batch))
            with _dirty_lock:
                _dirty_items.update(batch)
            _dirty_event.set()


# ===========================================================================
# Job 1: история продаж (topup)[cite: 6]
# ===========================================================================

def sanitize_table_name(item_id: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in item_id)
    return f"hist_{safe}"


def parse_time(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fetch_history_page(item_id: str, offset: int) -> dict:
    url = f"{BASE_URL}/{REGION}/auction/{item_id}/history"
    params = {"additional": "true", "limit": LIMIT, "offset": offset}
    return api_get_json(url, params, item_id, use_rate_limit=True)


def record_exists_in_db(table: str, row: dict) -> bool:
    try:
        conn = sqlite3.connect(HISTORY_DB, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000;")
        cur = conn.execute(
            f"""SELECT 1 FROM "{table}" WHERE
                time = ? AND price = ? AND
                COALESCE(qlt, -999999) = COALESCE(?, -999999) AND
                COALESCE(ptn, -999999) = COALESCE(?, -999999) AND
                COALESCE(stats_random, -999999.0) = COALESCE(?, -999999.0) AND
                upgrade_bonus = ? AND
                COALESCE(spawn_time, -999999) = COALESCE(?, -999999) AND
                COALESCE(it_transf_count, -999999) = COALESCE(?, -999999) AND
                COALESCE(bonus_json, '') = COALESCE(?, '') AND
                COALESCE(ndmg, -999999.0) = COALESCE(?, -999999.0) AND
                COALESCE(md_k, -999999.0) = COALESCE(?, -999999.0)
                LIMIT 1""",
            (row["time"], row["price"], row["qlt"], row["ptn"],
             row["stats_random"], row["upgrade_bonus"], row["spawn_time"],
             row["it_transf_count"], row["bonus_json"], row["ndmg"], row["md_k"]),
        )
        found = cur.fetchone() is not None
        conn.close()
        return found
    except sqlite3.OperationalError:
        return False


_history_write_queue: queue.Queue = queue.Queue(maxsize=2000)
_HISTORY_STOP = object()

_HISTORY_INSERT = """
    INSERT OR IGNORE INTO {table}
        (amount, price, time, qlt, ptn, stats_random, upgrade_bonus, spawn_time,
         it_transf_count, bonus_json, ndmg, md_k)
    VALUES (:amount, :price, :time, :qlt, :ptn, :stats_random, :upgrade_bonus, :spawn_time,
            :it_transf_count, :bonus_json, :ndmg, :md_k)
"""

_HISTORY_CREATE = """
    CREATE TABLE IF NOT EXISTS {table} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        amount INTEGER NOT NULL DEFAULT 1,
        price INTEGER NOT NULL,
        time TEXT NOT NULL,
        qlt INTEGER,
        ptn INTEGER,
        stats_random REAL,
        upgrade_bonus REAL NOT NULL DEFAULT 0.0,
        spawn_time INTEGER,
        it_transf_count INTEGER,
        bonus_json TEXT,
        ndmg REAL,
        md_k REAL
    )
"""

_HISTORY_UNIQUE_INDEX = """
    CREATE UNIQUE INDEX IF NOT EXISTS "uidx_{table}_full" ON {table} (
        time, price,
        COALESCE(qlt, -999999),
        COALESCE(ptn, -999999),
        COALESCE(stats_random, -999999.0),
        upgrade_bonus,
        COALESCE(spawn_time, -999999),
        COALESCE(it_transf_count, -999999),
        COALESCE(bonus_json, ''),
        COALESCE(ndmg, -999999.0),
        COALESCE(md_k, -999999.0)
    )
"""


def history_writer_loop() -> None:
    conn = sqlite3.connect(HISTORY_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    ensured: set[str] = set()

    while True:
        job = _history_write_queue.get()
        if job is _HISTORY_STOP:
            _history_write_queue.task_done()
            break
        try:
            table = job["table"]
            if table not in ensured:
                conn.execute(_HISTORY_CREATE.format(table=table))
                conn.execute(_HISTORY_UNIQUE_INDEX.format(table=table))
                conn.commit()
                ensured.add(table)
            cur = conn.executemany(_HISTORY_INSERT.format(table=table), job["rows"])
            conn.commit()
            log.info(
                "[%s] history offset=%d страница=%d новых=%d total_api=%s",
                job["item_id"], job["offset"], job["page_len"], cur.rowcount, job["total_reported"],
            )
        except Exception as e:
            log.error("[WRITER FAIL] %s offset=%s: %s", job.get("item_id"), job.get("offset"), e)
        finally:
            _history_write_queue.task_done()
    conn.close()


def collect_item_history_topup(item_id: str) -> str:
    table = sanitize_table_name(item_id)
    offset = 0
    pages = 0

    while True:
        if _shutdown.is_set():
            break
        data = fetch_history_page(item_id, offset)
        prices = data.get("prices", [])
        total_reported = data.get("total", 0)
        if not prices:
            break

        rows = []
        oldest_on_page = None
        for entry in prices:
            additional = entry.get("additional", {}) or {}
            row_time = parse_time(entry["time"])
            oldest_on_page = row_time if oldest_on_page is None else min(oldest_on_page, row_time)
            bonus = additional.get("bonus_properties")
            rows.append({
                "amount": entry.get("amount", 1),
                "price": entry.get("price"),
                "time": entry["time"],
                "qlt": additional.get("qlt"),
                "ptn": additional.get("ptn"),
                "stats_random": additional.get("stats_random"),
                "upgrade_bonus": additional.get("upgrade_bonus", 0.0),
                "spawn_time": additional.get("spawn_time"),
                "it_transf_count": additional.get("it_transf_count"),
                "bonus_json": json.dumps(bonus, ensure_ascii=False) if bonus is not None else None,
                "ndmg": additional.get("ndmg"),
                "md_k": additional.get("md_k"),
            })

        _history_write_queue.put({
            "item_id": item_id, "table": table, "rows": rows, "offset": offset,
            "page_len": len(rows), "total_reported": total_reported,
        })
        pages += 1

        stop = len(prices) < LIMIT
        if oldest_on_page is not None and oldest_on_page < CUTOFF_DATE:
            stop = True
        if not stop:
            oldest_row = rows[-1]
            if record_exists_in_db(table, oldest_row):
                stop = True
        if stop:
            break
        offset += LIMIT

    return f"{item_id}: страниц {pages} (offset до {offset})"


def job_history() -> None:
    if not _history_lock.acquire(blocking=False):
        log.warning("job_history: предыдущий прогон ещё идёт, пропуск")
        return

    started = time.monotonic()
    log.info("=== job_history: старт ===")
    try:
        item_ids = load_item_ids()
        log.info("Артефактов для topup: %d", len(item_ids))

        writer = threading.Thread(target=history_writer_loop, daemon=True)
        writer.start()

        failed: list[str] = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(collect_item_history_topup, iid): iid for iid in item_ids}
            for future in as_completed(futures):
                iid = futures[future]
                try:
                    result = future.result()
                    log.info("[OK] %s", result)
                except Exception as e:
                    log.error("[FAIL] %s: %s: %s", iid, type(e).__name__, e)
                    failed.append(iid)

        if failed:
            log.warning("Ошибки по %d артеfактам: %s", len(failed), failed[:10])

        _history_write_queue.join()
        _history_write_queue.put(_HISTORY_STOP)
        writer.join()

        elapsed = time.monotonic() - started
        log.info("=== job_history: готово за %.0fс, ошибок: %d ===", elapsed, len(failed))
    finally:
        _history_lock.release()


# ===========================================================================
# Job 2: активные лоты (атомарный снимок)[cite: 6]
# ===========================================================================

_ACTIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS fetch_runs (
    id          INTEGER PRIMARY KEY,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    lots_count  INTEGER DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS active_lots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id       TEXT NOT NULL,
    amount        INTEGER DEFAULT 1,
    start_price   INTEGER,
    buyout_price  INTEGER,
    start_time    TEXT,
    end_time      TEXT,
    qlt           INTEGER,
    ptn           INTEGER,
    stats_random  REAL,
    upgrade_bonus REAL,
    bonus_json    TEXT,
    extra_json    TEXT,
    fetch_run_id  INTEGER NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL,
    lot_key       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',
    FOREIGN KEY (fetch_run_id) REFERENCES fetch_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_active_item ON active_lots(item_id);
CREATE INDEX IF NOT EXISTS idx_active_end  ON active_lots(end_time);

CREATE TABLE IF NOT EXISTS collector_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

_ACTIVE_UPSERT = """
INSERT INTO active_lots (
    item_id, amount, start_price, buyout_price,
    start_time, end_time, qlt, ptn, stats_random, upgrade_bonus,
    bonus_json, extra_json, fetch_run_id, first_seen_at, last_seen_at, lot_key
) VALUES (
    :item_id, :amount, :start_price, :buyout_price,
    :start_time, :end_time, :qlt, :ptn, :stats_random, :upgrade_bonus,
    :bonus_json, :extra_json, :fetch_run_id, :first_seen_at, :last_seen_at, :lot_key
)
ON CONFLICT(lot_key) DO UPDATE SET
    item_id       = excluded.item_id,
    amount        = excluded.amount,
    start_price   = excluded.start_price,
    buyout_price  = excluded.buyout_price,
    start_time    = excluded.start_time,
    end_time      = excluded.end_time,
    qlt           = excluded.qlt,
    ptn           = excluded.ptn,
    stats_random  = excluded.stats_random,
    upgrade_bonus = excluded.upgrade_bonus,
    bonus_json    = excluded.bonus_json,
    extra_json    = excluded.extra_json,
    fetch_run_id  = excluded.fetch_run_id,
    last_seen_at  = excluded.last_seen_at,
    first_seen_at = active_lots.first_seen_at,
    status        = 'active'
"""


def migrate_active_schema(conn: sqlite3.Connection) -> None:
    columns = [row[1] for row in conn.execute("PRAGMA table_info(active_lots)")]
    if "lot_key" not in columns:
        conn.execute("ALTER TABLE active_lots ADD COLUMN lot_key TEXT NOT NULL DEFAULT ''")
    if "status" not in columns:
        conn.execute("ALTER TABLE active_lots ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")

    conn.execute("SELECT COUNT(*) FROM active_lots WHERE lot_key IS NULL OR lot_key = ''")
    empty_count = conn.execute("SELECT COUNT(*) FROM active_lots WHERE lot_key IS NULL OR lot_key = ''").fetchone()[0]
    if empty_count:
        rows = conn.execute(
            "SELECT rowid, item_id, start_time, end_time, qlt, spawn_time FROM active_lots WHERE lot_key IS NULL OR lot_key = ''"
        ).fetchall()
        for rowid, item_id, start_time, end_time, qlt, spawn_time in rows:
            qlt_part = "qlt=None" if qlt is None else f"qlt={qlt}"
            base = f"{item_id}|{start_time or ''}|{end_time or ''}|{qlt_part}"
            if spawn_time is not None and base in {"|", "| | |qlt=None"}:
                base = f"{item_id}|spawn={spawn_time}"
            conn.execute(
                "UPDATE active_lots SET lot_key = ? WHERE rowid = ?",
                (base, rowid),
            )

    cursor = conn.execute("SELECT rowid, lot_key FROM active_lots")
    all_rows = cursor.fetchall()
    seen: set[str] = set()
    for index, (rowid, key) in enumerate(all_rows):
        if key in seen:
            candidate = f"{key}|dup{index}"
            conn.execute("UPDATE active_lots SET lot_key = ? WHERE rowid = ?", (candidate, rowid))
        else:
            seen.add(key)

    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_active_lot_key ON active_lots(lot_key)")
    conn.commit()


def init_active_db(conn: sqlite3.Connection) -> None:
    conn.executescript(_ACTIVE_SCHEMA)
    conn.commit()
    migrate_active_schema(conn)


def fetch_lots_page(item_id: str, offset: int) -> dict:
    url = f"{BASE_URL}/{REGION}/auction/{item_id}/lots"
    params = {
        "additional": "true",
        "limit": LIMIT,
        "offset": offset,
        "sort": SORT,
        "order": ORDER,
    }
    # Отключаем rate limit, работаем через естественную задержку потоков
    return api_get_json(url, params, item_id, use_rate_limit=False)


def fetch_all_lots_for_item(item_id: str) -> list[dict]:
    collected: list[dict] = []
    offset = 0
    while True:
        if _shutdown.is_set():
            raise InterruptedError("shutdown")
        data = fetch_lots_page(item_id, offset)
        lots = data.get("lots", [])
        total = data.get("total", 0)
        collected.extend(lots)
        if not lots or len(collected) >= total:
            break
        offset += len(lots)
    return collected


def lot_key(lot: dict, item_id: str) -> str | None:
    additional = lot.get("additional", {}) or {}
    qlt = additional.get("qlt")
    start_time = lot.get("startTime")
    end_time = lot.get("endTime")
    if not start_time or not end_time:
        return None

    qlt_part = "qlt=None" if qlt is None else f"qlt={qlt}"
    return f"{item_id}|{start_time}|{end_time}|{qlt_part}"


def lot_to_row(lot: dict, item_id: str, run_id: int, now: str) -> dict | None:
    additional = lot.get("additional", {}) or {}
    key = lot_key(lot, item_id)
    if key is None:
        log.warning("[%s] лот без startTime/endTime, пропуск", item_id)
        return None

    buyout_price = lot.get("buyoutPrice")
    if not buyout_price or buyout_price <= 0:
        return None

    bonus = additional.get("bonus_properties")
    bonus_json = json.dumps(bonus, ensure_ascii=False) if bonus is not None else None
    extra = {k: v for k, v in additional.items() if k not in KNOWN_ADDITIONAL_FIELDS}
    extra_json = json.dumps(extra, ensure_ascii=False) if extra else None

    return {
        "lot_key": key,
        "item_id": lot.get("itemId", item_id),
        "amount": lot.get("amount", 1),
        "start_price": lot.get("startPrice"),
        "buyout_price": lot.get("buyoutPrice"),
        "start_time": lot.get("startTime"),
        "end_time": lot.get("endTime"),
        "qlt": additional.get("qlt"),
        "ptn": additional.get("ptn"),
        "stats_random": additional.get("stats_random"),
        "upgrade_bonus": additional.get("upgrade_bonus"),
        "bonus_json": bonus_json,
        "extra_json": extra_json,
        "fetch_run_id": run_id,
        "first_seen_at": now,
        "last_seen_at": now,
    }


def record_failed_active_run(conn: sqlite3.Connection, started_at: str, error_msg: str) -> None:
    conn.execute(
        "INSERT INTO fetch_runs (started_at, finished_at, lots_count, status) VALUES (?, ?, 0, 'error')",
        (started_at, utc_now_iso()),
    )
    conn.execute(
        "INSERT OR REPLACE INTO collector_state (key, value) VALUES ('last_active_run_error', ?)",
        (error_msg[:500],),
    )
    conn.commit()


def job_active_lots() -> None:
    if not _active_lock.acquire(blocking=False):
        log.warning("job_active_lots: предыдущий прогон ещё идёт, пропуск")
        return

    started = time.monotonic()
    started_at = utc_now_iso()
    log.info("=== job_active_lots: старт (потоковая запись по мере готовности) ===")

    set_collector_status("Опрашиваем API (в процессе...)")

    conn = sqlite3.connect(ACTIVE_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    init_active_db(conn)

    try:
        item_ids = load_item_ids()
        log.info("Артефактов для опроса: %d", len(item_ids))

        cur = conn.execute(
            "INSERT INTO fetch_runs (started_at, status) VALUES (?, 'running')",
            (started_at,),
        )
        run_id = cur.lastrowid
        conn.commit()

        total_lots = 0
        skipped_no_buyout = 0
        failed_items: list[str] = []
        now = utc_now_iso()

        # Каждый айтем пишется в БД СРАЗУ по готовности (не ждём остальных),
        # и сразу помечается "грязным" для инкрементального пересчёта мозга
        # (см. mark_dirty/brain_worker_loop). "gone" теперь определяется по
        # ЭТОМУ конкретному айтему, а не по всему run_id разом — иначе лоты
        # ещё не опрошенных айтемов ошибочно пометились бы пропавшими.
        with ThreadPoolExecutor(max_workers=ACTIVE_MAX_WORKERS) as pool:
            futures = {pool.submit(fetch_all_lots_for_item, iid): iid for iid in item_ids}
            for future in as_completed(futures):
                iid = futures[future]
                try:
                    lots = future.result()
                except Exception as e:
                    log.error("[FAIL] %s: %s: %s", iid, type(e).__name__, e)
                    failed_items.append(iid)
                    continue

                rows = []
                for lot in lots:
                    row = lot_to_row(lot, iid, run_id, now)
                    if row:
                        rows.append(row)
                    else:
                        skipped_no_buyout += 1

                try:
                    conn.execute("BEGIN IMMEDIATE")
                    if rows:
                        conn.executemany(_ACTIVE_UPSERT, rows)
                    conn.execute(
                        "UPDATE active_lots SET status = 'gone' "
                        "WHERE item_id = ? AND fetch_run_id != ? AND status = 'active'",
                        (iid, run_id),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    log.exception("[%s] ошибка записи в БД, айтем пропущен в этом цикле", iid)
                    failed_items.append(iid)
                    continue

                total_lots += len(rows)
                log.info("[%s] %d лотов записаны", iid, len(rows))
                mark_dirty(iid)

        finished_at = utc_now_iso()
        if failed_items:
            status = "partial"
            conn.execute(
                "INSERT OR REPLACE INTO collector_state (key, value) VALUES ('last_active_run_error', ?)",
                (f"ошибки по {len(failed_items)} артефактам: {failed_items[:5]}",),
            )
            set_collector_status(f"Частично: {len(failed_items)} артефактов не обновлены")
        else:
            status = "ok"
            conn.execute(
                "INSERT OR REPLACE INTO collector_state (key, value) VALUES ('last_active_run', ?)",
                (finished_at,),
            )
            set_collector_status("Ожидание (спим до следующей минуты)...")

        conn.execute(
            "UPDATE fetch_runs SET status=?, finished_at=?, lots_count=? WHERE id=?",
            (status, finished_at, total_lots, run_id),
        )
        conn.commit()

        elapsed = time.monotonic() - started
        log.info(
            "=== job_active_lots: готово за %.0fс, лотов=%d, отсеяно=%d, ошибок=%d ===",
            elapsed, total_lots, skipped_no_buyout, len(failed_items),
        )

        # Каждый айтем уже пересчитан по мере готовности через mark_dirty()
        # выше — здесь просто финальный SSE-пинг на случай, если что-то из
        # частичных пересчётов ещё не долетело до фронта.
        notify_server()

    except Exception:
        conn.rollback()
        record_failed_active_run(conn, started_at, "необработанная ошибка job_active_lots")
        log.exception("=== job_active_lots: ошибка ===")
        set_collector_status("Ошибка сбора!")
    finally:
        conn.close()
        _active_lock.release()
# ===========================================================================

def run_once(mode: str | None) -> None:
    if mode in (None, "history"):
        job_history()
    if mode in (None, "active"):
        job_active_lots()
        # В одноразовом режиме процесс сразу завершится, поэтому brain_worker_loop
        # не успеет обработать дебаунс-очередь — досчитываем СИНХРОННО (не через
        # trigger_brain_recompute — тот асинхронный, процесс успел бы выйти раньше).
        global _brain_inited
        if not _brain_inited:
            brain_module.setup_logging()
            _brain_inited = True
        try:
            brain_module.recompute_all()
        except Exception:
            log.exception("Ошибка в brain recompute (--once)")


# ---- Конфиг интервалов ----
HISTORY_INTERVAL_SEC = 3600  # 1 час между обновлениями истории
BUFFER_DELAY_SEC = 60        # 1 минута паузы между фазами
ACTIVE_INTERVAL_SEC = 60     # Интревал сканирования активных лотов


def daemon_loop() -> None:
    log.info(
        "Daemon запущен (последовательный режим: история раз в %d сек, буфер %d сек)",
        HISTORY_INTERVAL_SEC, BUFFER_DELAY_SEC,
    )

    # Фоновый воркер инкрементального пересчёта мозга — обрабатывает mark_dirty()
    # из job_active_lots() по мере готовности каждого айтема, не дожидаясь конца
    # всего цикла опроса.
    threading.Thread(target=brain_worker_loop, daemon=True, name="brain-worker").start()

    # ----------------------------------------------------
    # 1. СТАРТ: Первый прогон истории при запуске
    # ----------------------------------------------------
    # Авто-очистка проданных лотов от прошлой сессии
    try:
        conn = sqlite3.connect(ACTIVE_DB, timeout=10)
        init_active_db(conn)
        gone = conn.execute("DELETE FROM active_lots WHERE status = 'gone'").rowcount
        conn.commit()
        conn.close()
        if gone:
            log.info("Авто-очистка: удалено %d проданных лотов от прошлой сессии", gone)
    except Exception as e:
        log.error("Авто-очистка не удалась: %s", e)

    log.info("=== [СТАРТ] Первоначальное дособирание истории ===")
    set_collector_status("Старт: проверка и досбор истории...")
    job_history()
    
    last_history_time = time.monotonic()

    # Пауза в 1 минуту перед началом сканирования активных
    if not _shutdown.is_set():
        log.info("Пауза %d сек перед переходом к активным лотам...", BUFFER_DELAY_SEC)
        set_collector_status(f"Пауза {BUFFER_DELAY_SEC}с перед стартом активных...")
        _shutdown.wait(timeout=BUFFER_DELAY_SEC)

    # ----------------------------------------------------
    # 2. БЕСКОНЕЧНЫЙ ПОСЛЕДОВАТЕЛЬНЫЙ ЦИКЛ
    # ----------------------------------------------------
    while not _shutdown.is_set():
        now = time.monotonic()

        # Проверяем, прошёл ли 1 час с момента последнего сбора истории
        if now - last_history_time >= HISTORY_INTERVAL_SEC:
            log.info("=== [ЧАСОВОЙ ЦИКЛ] Начинаем плановое обновление истории ===")
            set_collector_status("Часовой сбор истории...")
            
            # Выполняем сбор истории СИНХРОННО (без потоков)
            job_history()
            last_history_time = time.monotonic()

            # Пауза 1 минута после сбора истории
            if not _shutdown.is_set():
                log.info("История обновлена. Пауза %d сек перед активными лотами...", BUFFER_DELAY_SEC)
                set_collector_status(f"Пауза {BUFFER_DELAY_SEC}с после обновления истории...")
                _shutdown.wait(timeout=BUFFER_DELAY_SEC)

        else:
            # Обычный скан активных лотов
            cycle_start = time.monotonic()
            job_active_lots()

            # Вычисляем оставшееся время до конца 60-секундного цикла
            elapsed = time.monotonic() - cycle_start
            sleep_time = max(0.0, ACTIVE_INTERVAL_SEC - elapsed)

            if sleep_time > 0 and not _shutdown.is_set():
                log.info("Спим %.1f сек до следующей минуты...", sleep_time)
                _shutdown.wait(timeout=sleep_time)

    log.info("Daemon остановлен")


def _handle_signal(signum, _frame) -> None:
    log.info("Получен сигнал %s, завершение...", signum)
    _shutdown.set()


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="STALZONE auction data collector")
    parser.add_argument(
        "--once",
        nargs="?",
        const="all",
        choices=["all", "history", "active"],
        help="один цикл (all/history/active) и выход",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    if args.once:
        mode = None if args.once == "all" else args.once
        run_once(mode)
    else:
        daemon_loop()


if __name__ == "__main__":
    main()