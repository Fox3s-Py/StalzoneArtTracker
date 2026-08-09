"""
Сбор истории аукциона STALZONE по артефактам в SQLite + окно прогресса.

# deprecated: use collector.py (job_history в режиме topup, без GUI)

Три режима сбора по артефакту:
- "full"          — полный проход с offset=0 до CUTOFF_DATE, невзирая на то, что уже есть в БД.
                    Используется для явно указанного списка артефактов (FORCE_FULL_ITEMS) —
                    обычно это те, что раньше упали с ошибкой и не факт что докачались.
- "topup"         — идём с offset=0, но останавливаемся, как только встречаем на странице
                    запись, которая уже есть в БД (по полному набору полей). Это значит, что
                    дальше (в прошлое) всё уже собрано — нет смысла перекачивать всё заново.
- "topup_verify"  — сначала topup, затем проверка: сравниваем кол-во записей в БД с total
                    из API. Если в БД меньше 50% от total — запускаем полный пересбор (full).
                    Режим по умолчанию для всех артефактов, кроме FORCE_FULL_ITEMS.

Дедупликация — по полному набору полей: time → price → все параметры additional.
Два лота считаются одинаковыми только если совпадают ВСЕ поля. Для этого используется
выражаемый unique-индекс с COALESCE (SQLite считает NULL != NULL, поэтому NULL-поля
заменяются на sentinel-значения). Дополнительно INSERT OR IGNORE — безопасный фолбэк.

Запись в БД:
- воркеры (по одному на артефакт) занимаются только сетью и парсингом
- один выделенный поток-писатель забирает пачки строк из очереди и пишет их в БД

Окно прогресса:
- tkinter-окно в главном потоке, вся сама сборка идёт в фоновом потоке
"""

import json
import queue
import sqlite3
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from config import CLIENT_ID, CLIENT_SECRET

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

# ---- Конфиг ----

REGION = "RU"
BASE_URL = "https://eapi.stalzone.com"
LISTING_URL = "https://raw.githubusercontent.com/EXBO-Studio/stalzone-database/main/ru/listing.json"

LIMIT = 200
CUTOFF_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
MAX_WORKERS = 5
DB_PATH = "auction_history.sqlite3"

MIN_REQUEST_INTERVAL = 0.5
MAX_RETRIES = 5            # для обычных сетевых сбоев (в т.ч. битый JSON в ответе)
MAX_RETRIES_429 = 8

# Артефакты, которые нужно пройти ПОЛНОСТЬЮ заново (с нуля до CUTOFF_DATE),
# невзирая на то, что уже частично есть в БД.
FORCE_FULL_ITEMS = {
    
}

_rate_lock = threading.Lock()
_last_request_time = 0.0


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


def sanitize_table_name(item_id: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in item_id)
    return f"hist_{safe}"


def fetch_items_list() -> list[str]:
    resp = requests.get(LISTING_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    items = []
    for entry in data:
        path = entry.get("data", "")
        if path.startswith("/items/artefact"):
            item_id = path.rsplit("/", 1)[-1].removesuffix(".json")
            items.append(item_id)
    return items


def fetch_history_page(item_id: str, offset: int) -> dict:
    url = f"{BASE_URL}/{REGION}/auction/{item_id}/history"
    params = {"additional": "true", "limit": LIMIT, "offset": offset}
    attempt_429 = 0
    attempt_other = 0
    while True:
        rate_limited_wait()
        try:
            resp = requests.get(url, headers=headers(), params=params, timeout=15)
            if resp.status_code == 429:
                attempt_429 += 1
                if attempt_429 > MAX_RETRIES_429:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else min(60, 2 ** attempt_429)
                print(f"[{item_id}] 429, жду {wait:.1f}с (попытка {attempt_429}/{MAX_RETRIES_429})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            try:
                return resp.json()
            except (json.JSONDecodeError, ValueError) as e:
                raise requests.RequestException(f"невалидный JSON в ответе: {e}") from e
        except requests.RequestException as e:
            attempt_other += 1
            if attempt_other >= MAX_RETRIES:
                raise type(e)(f"[{item_id}] после {MAX_RETRIES} попыток: {e}") from e
            time.sleep(1.5 * attempt_other)


def parse_time(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def record_exists_in_db(table: str, row: dict) -> bool:
    """Проверяет, есть ли уже такая запись в БД — по полному набору полей
    (time → price → все параметры additional). Используется в режиме topup,
    чтобы понять, что дальше (в прошлое) уже всё собрано и можно остановиться."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
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
        return False  # таблицы ещё нет — точно не собрано


def count_records_in_db(table: str) -> int:
    """Считает количество записей в таблице истории для предмета."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000;")
        cur = conn.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cur.fetchone()[0]
        conn.close()
        return count
    except sqlite3.OperationalError:
        return 0


# ---------------------------------------------------------------------------
# Единственный поток-писатель
# ---------------------------------------------------------------------------

write_queue: "queue.Queue" = queue.Queue(maxsize=2000)
_STOP = object()

INSERT_SQL_TEMPLATE = """
    INSERT OR IGNORE INTO {table}
        (amount, price, time, qlt, ptn, stats_random, upgrade_bonus, spawn_time,
         it_transf_count, bonus_json, ndmg, md_k)
    VALUES (:amount, :price, :time, :qlt, :ptn, :stats_random, :upgrade_bonus, :spawn_time,
            :it_transf_count, :bonus_json, :ndmg, :md_k)
"""

CREATE_TABLE_TEMPLATE = """
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

# Уникальный индекс по полному набору полей с COALESCE для NULL-безопасности.
# SQLite считает NULL != NULL, поэтому NULL-поля заменяются на sentinel-значения.
UNIQUE_INDEX_TEMPLATE = """
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


def writer_loop() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    ensured_tables: set[str] = set()

    while True:
        job = write_queue.get()
        if job is _STOP:
            write_queue.task_done()
            break
        try:
            table = job["table"]
            if table not in ensured_tables:
                conn.execute(CREATE_TABLE_TEMPLATE.format(table=table))
                conn.execute(UNIQUE_INDEX_TEMPLATE.format(table=table))
                conn.commit()
                ensured_tables.add(table)
            cur = conn.executemany(INSERT_SQL_TEMPLATE.format(table=table), job["rows"])
            conn.commit()
            inserted = cur.rowcount
            print(f"[{job['item_id']}] offset={job['offset']} страница={job['page_len']} "
                  f"новых={inserted} total_api={job['total_reported']} самая старая={job['oldest_on_page']}")
        except Exception as e:
            print(f"[WRITER FAIL] {job.get('item_id')} offset={job.get('offset')}: {e}")
        finally:
            write_queue.task_done()

    conn.close()


# ---------------------------------------------------------------------------
# Общее состояние прогресса для окна
# ---------------------------------------------------------------------------

progress_lock = threading.Lock()
item_progress: dict[str, dict] = {}


def set_progress(item_id: str, **kwargs) -> None:
    with progress_lock:
        item_progress.setdefault(item_id, {}).update(kwargs)


def compute_fraction(start_now: datetime, oldest_on_page: datetime | None) -> float:
    if oldest_on_page is None:
        return 0.0
    span_total = (start_now - CUTOFF_DATE).total_seconds()
    span_done = (start_now - oldest_on_page).total_seconds()
    if span_total <= 0:
        return 1.0
    return max(0.0, min(1.0, span_done / span_total))


# ---------------------------------------------------------------------------
# Воркеры
# ---------------------------------------------------------------------------

def collect_item_history(item_id: str, mode: str = "full") -> str:
    table = sanitize_table_name(item_id)
    offset = 0
    total_pages_sent = 0
    start_now = datetime.now(timezone.utc)

    set_progress(item_id, status="в работе", fraction=0.0, offset=0, total_reported=None, oldest=None)

    while True:
        data = fetch_history_page(item_id, offset)
        prices = data.get("prices", [])
        total_reported = data.get("total", 0)

        set_progress(item_id, total_reported=total_reported)

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

        write_queue.put({
            "item_id": item_id, "table": table, "rows": rows, "offset": offset,
            "page_len": len(rows), "total_reported": total_reported, "oldest_on_page": oldest_on_page,
        })
        total_pages_sent += 1

        fraction = compute_fraction(start_now, oldest_on_page)
        set_progress(item_id, status="в работе", fraction=fraction, offset=offset,
                     total_reported=total_reported, oldest=oldest_on_page)

        stop = False
        if len(prices) < LIMIT:
            stop = True
        if oldest_on_page is not None and oldest_on_page < CUTOFF_DATE:
            stop = True

        if not stop and mode == "topup":
            # самая старая запись на этой странице — если она уже есть в БД,
            # значит всё, что дальше (глубже в прошлое), тоже уже собрано
            oldest_row = rows[-1]
            if record_exists_in_db(table, oldest_row):
                stop = True

        if stop:
            break

        offset += LIMIT

    set_progress(item_id, status="готово", fraction=1.0)
    mode_label = "полностью" if mode == "full" else "топ-ап"
    return f"{item_id} [{mode_label}]: отправлено на запись страниц {total_pages_sent} (офсет дошёл до {offset})"


# ---------------------------------------------------------------------------
# Окно прогресса
# ---------------------------------------------------------------------------

class ProgressGUI:
    def __init__(self, items: list[str]):
        self.items = items
        self.start_time = time.monotonic()
        self.root = tk.Tk()
        self.root.title("Сбор истории аукциона STALZONE")
        self.root.geometry("820x560")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.closed = False

        top = ttk.Frame(self.root, padding=10)
        top.pack(fill="x")

        self.overall_label = ttk.Label(top, text="Общий прогресс: 0%", font=("Segoe UI", 11, "bold"))
        self.overall_label.pack(anchor="w")

        self.overall_bar = ttk.Progressbar(top, orient="horizontal", mode="determinate", maximum=100)
        self.overall_bar.pack(fill="x", pady=(4, 4))

        self.eta_label = ttk.Label(top, text="Готовых артефактов: 0 / 0   ETA: —")
        self.eta_label.pack(anchor="w")

        columns = ("item", "mode", "status", "progress", "offset", "total_api", "oldest")
        self.tree = ttk.Treeview(self.root, columns=columns, show="headings", height=25)
        headers_map = {
            "item": "Артефакт", "mode": "Режим", "status": "Статус", "progress": "%",
            "offset": "Offset", "total_api": "Total (API)", "oldest": "Самая старая запись",
        }
        widths = {"item": 80, "mode": 70, "status": 90, "progress": 55, "offset": 80, "total_api": 100, "oldest": 190}
        for col in columns:
            self.tree.heading(col, text=headers_map[col])
            self.tree.column(col, width=widths[col], anchor="center")
        self.tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        for item_id in items:
            mode_label = "full" if item_id in FORCE_FULL_ITEMS else "topup+verify"
            self.tree.insert("", "end", iid=item_id,
                              values=(item_id, mode_label, "ожидание", "0%", "-", "-", "-"))

        self.root.after(500, self.refresh)

    def _on_close(self) -> None:
        self.closed = True
        self.root.destroy()

    def refresh(self) -> None:
        if self.closed:
            return
        with progress_lock:
            snapshot = {k: dict(v) for k, v in item_progress.items()}

        done_count = 0
        fractions = []
        for item_id in self.items:
            data = snapshot.get(item_id)
            if not data:
                continue
            fraction = data.get("fraction", 0.0)
            fractions.append(fraction)
            status = data.get("status", "ожидание")
            if status == "готово":
                done_count += 1
            oldest = data.get("oldest")
            oldest_str = oldest.strftime("%Y-%m-%d %H:%M") if oldest else "-"
            total_reported = data.get("total_reported")
            total_str = str(total_reported) if total_reported is not None else "-"
            offset = data.get("offset")
            offset_str = str(offset) if offset is not None else "-"
            mode_label = "full" if item_id in FORCE_FULL_ITEMS else "topup+verify"
            try:
                self.tree.item(item_id, values=(
                    item_id, mode_label, status, f"{fraction*100:.0f}%", offset_str, total_str, oldest_str
                ))
            except tk.TclError:
                pass

        total_items = len(self.items)
        overall_fraction = (sum(fractions) / total_items) if total_items else 0.0
        self.overall_bar["value"] = overall_fraction * 100
        self.overall_label.config(text=f"Общий прогресс: {overall_fraction*100:.1f}%")

        elapsed = time.monotonic() - self.start_time
        if overall_fraction > 0.01:
            estimated_total_time = elapsed / overall_fraction
            remaining = max(0, estimated_total_time - elapsed)
            eta_str = self._format_duration(remaining)
        else:
            eta_str = "считаю..."

        self.eta_label.config(text=f"Готовых артефактов: {done_count} / {total_items}   ETA: {eta_str}")

        try:
            self.root.after(500, self.refresh)
        except tk.TclError:
            pass

    @staticmethod
    def _format_duration(seconds: float) -> str:
        seconds = int(seconds)
        hours, rem = divmod(seconds, 3600)
        minutes, secs = divmod(rem, 60)
        if hours:
            return f"{hours}ч {minutes}м"
        if minutes:
            return f"{minutes}м {secs}с"
        return f"{secs}с"

    def run(self) -> None:
        self.root.mainloop()


# ---------------------------------------------------------------------------
# Оркестрация
# ---------------------------------------------------------------------------

def run_collection(items_with_mode: list[tuple[str, str]]) -> None:
    writer = threading.Thread(target=writer_loop, daemon=True)
    writer.start()

    # Разделяем предметы по режиму
    full_items = [iid for iid, mode in items_with_mode if mode == "full"]
    verify_items = [iid for iid, mode in items_with_mode if mode == "topup_verify"]

    failed_items: list[tuple[str, str]] = []

    # Фаза 1: topup для verify-предметов + full для full-предметов
    phase1 = [(iid, "full") for iid in full_items] + [(iid, "topup") for iid in verify_items]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(collect_item_history, iid, mode): (iid, mode) for iid, mode in phase1}
        for future in as_completed(futures):
            iid, mode = futures[future]
            try:
                result = future.result()
                print(f"[OK] {result}")
            except Exception as e:
                print(f"[FAIL] {iid}: {type(e).__name__}: {e}")
                set_progress(iid, status="ошибка")
                failed_items.append((iid, mode))

    # Ждём, пока очередь записи опустеет — чтобы count_records_in_db был точным
    write_queue.join()

    # Фаза 2: проверка полноты для topup_verify-предметов
    # Сравниваем кол-во записей в БД с total из API.
    # Если в БД меньше 50% от total — запускаем полный пересбор (deep check).
    recheck_items: list[str] = []
    for iid in verify_items:
        table = sanitize_table_name(iid)
        db_count = count_records_in_db(table)
        total = item_progress.get(iid, {}).get("total_reported") or 0
        if total > 0 and db_count < total * 0.5:
            print(f"[VERIFY] {iid}: DB={db_count}, API total={total}, <50% — глубокая проверка")
            set_progress(iid, status="глубокая проверка", fraction=0.0)
            recheck_items.append(iid)
        else:
            print(f"[VERIFY] {iid}: DB={db_count}, API total={total} — OK")
            set_progress(iid, status="готово", fraction=1.0)

    # Глубокая проверка (full) для предметов, не прошедших 50% порог
    if recheck_items:
        print(f"\nГлубокая проверка для {len(recheck_items)} предметов...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(collect_item_history, iid, "full"): iid for iid in recheck_items}
            for future in as_completed(futures):
                iid = futures[future]
                try:
                    result = future.result()
                    print(f"[OK recheck] {result}")
                except Exception as e:
                    print(f"[FAIL recheck] {iid}: {type(e).__name__}: {e}")
                    set_progress(iid, status="ошибка")
                    failed_items.append((iid, "full"))

    # Повторный проход по упавшим
    if failed_items:
        print(f"\nПовторный проход по недобитым артефактам ({len(failed_items)} шт), последовательно...")
        still_failed = []
        for iid, mode in failed_items:
            try:
                result = collect_item_history(iid, mode)
                print(f"[OK повтор] {result}")
            except Exception as e:
                print(f"[FAIL повтор] {iid}: {type(e).__name__}: {e}")
                set_progress(iid, status="ошибка")
                still_failed.append(iid)
        if still_failed:
            print(f"\nТак и не собрались: {still_failed}")

    write_queue.join()
    write_queue.put(_STOP)
    writer.join()
    print("Сбор завершён.")


def main() -> None:
    print("Получаю список артефактов...")
    all_items = fetch_items_list()
    print(f"Найдено артефактов: {len(all_items)}")

    items_with_mode = [
        (item_id, "full" if item_id in FORCE_FULL_ITEMS else "topup_verify")
        for item_id in all_items
    ]
    forced_count = sum(1 for _, m in items_with_mode if m == "full")
    print(f"Полный сбор (принудительно, список): {forced_count}")
    print(f"Топ-ап + проверка 50%: {len(items_with_mode) - forced_count}")

    worker_thread = threading.Thread(target=run_collection, args=(items_with_mode,), daemon=True)
    worker_thread.start()

    gui = ProgressGUI(all_items)
    gui.run()


if __name__ == "__main__":
    main()