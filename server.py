from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from urllib.parse import unquote, urlparse
from collections import deque

sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)  # type: ignore[attr-defined]

PORT = 8000
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

ACTIVE_DB = os.path.join(BASE_DIR, "auction_active.sqlite3")
HISTORY_DB = os.path.join(BASE_DIR, "auction_history.sqlite3")
SCORES_DB = os.path.join(BASE_DIR, "auction_scores.sqlite3")
ITEMS_PATH = os.path.join(BASE_DIR, "items.json")
BRAIN_CONFIG_PATH = os.path.join(BASE_DIR, "brain_config.json")


def sanitize_table_name(item_id: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in item_id)
    return f"hist_{safe}"


# Подключённые SSE-клиенты (браузеры). Защищено lock'ом.
_sse_clients: deque = deque()
_sse_lock = threading.Lock()

# Текущий прогресс скана/расчёта — держим последнее известное состояние в
# памяти, чтобы вкладка, открытая посреди цикла, сразу увидела актуальную
# картину (не только новые тики через SSE).
_scan_progress: dict = {"total": 0, "fetched": 0, "computed": 0, "active": False}
_scan_progress_lock = threading.Lock()


def get_last_active_run() -> str | None:
    """Время последнего успешного снимка активных лотов (коллектор)."""
    try:
        conn = sqlite3.connect(ACTIVE_DB, timeout=10)
        row = conn.execute(
            "SELECT value FROM collector_state WHERE key='last_active_run'"
        ).fetchone()
        conn.close()
        return row[0] if row else None
    except sqlite3.Error:
        return None


def broadcast_update() -> None:
    """Рассылает SSE-событие 'data-updated' всем подключённым браузерам."""
    with _sse_lock:
        stale = []
        for wfile in _sse_clients:
            try:
                wfile.write(b"event: data-updated\ndata: {}\n\n")
                wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                stale.append(wfile)
        for wfile in stale:
            _sse_clients.remove(wfile)


def broadcast_progress(payload: dict) -> None:
    """Рассылает SSE-событие 'scan-progress' — лёгкий тик прогресса скана/
    расчёта (total/fetched/computed), НЕ вызывает полную перезагрузку
    данных на фронте (в отличие от 'data-updated')."""
    data = json.dumps(payload, ensure_ascii=False)
    message = f"event: scan-progress\ndata: {data}\n\n".encode("utf-8")
    with _sse_lock:
        stale = []
        for wfile in _sse_clients:
            try:
                wfile.write(message)
                wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                stale.append(wfile)
        for wfile in stale:
            _sse_clients.remove(wfile)


def load_items() -> dict:
    if not os.path.exists(ITEMS_PATH):
        return {}
    with open(ITEMS_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_scores() -> dict[str, dict]:
    """Кэш brain.py: lot_key -> {fairValue, absoluteProfit, ...}. Пусто, если
    brain.py ещё ни разу не отработал (auction_scores.sqlite3 нет)."""
    if not os.path.exists(SCORES_DB):
        return {}

    conn = sqlite3.connect(SCORES_DB)
    conn.row_factory = sqlite3.Row
    try:
        try:
            rows = conn.execute(
                """
                SELECT lot_key, fair_value, absolute_profit, percent_profit,
                       expected_days_to_sell, lambda_per_day, lambda_effective,
                       target_price, competitors_below, confidence, score, low_confidence,
                       pass_filter, reject_reason, next_tier_ptn, next_tier_price
                FROM lot_scores
                """
            ).fetchall()
        except sqlite3.OperationalError:
            # Старая схема без новых колонок — fallback с дефолтными значениями
            rows = conn.execute(
                """
                SELECT lot_key, fair_value, absolute_profit, percent_profit,
                       expected_days_to_sell, lambda_per_day, confidence, score, low_confidence,
                       pass_filter, reject_reason
                FROM lot_scores
                """
            ).fetchall()
            result = {}
            for row in rows:
                result[row["lot_key"]] = {
                    "fairValue": row["fair_value"],
                    "absoluteProfit": row["absolute_profit"],
                    "percentProfit": row["percent_profit"],
                    "expectedDaysToSell": row["expected_days_to_sell"],
                    "salesPerDay": row["lambda_per_day"],
                    "lambdaEffective": row["lambda_per_day"],
                    "targetPrice": row["fair_value"],
                    "competitorsBelow": 0,
                    "confidence": row["confidence"],
                    "score": row["score"],
                    "lowConfidence": bool(row["low_confidence"]),
                    "passFilter": bool(row["pass_filter"]),
                    "rejectReason": row["reject_reason"],
                    "nextTierPtn": None,
                    "nextTierPrice": None,
                }
            return result
    finally:
        conn.close()

    return {
        row["lot_key"]: {
            "fairValue": row["fair_value"],
            "absoluteProfit": row["absolute_profit"],
            "percentProfit": row["percent_profit"],
            "expectedDaysToSell": row["expected_days_to_sell"],
            "salesPerDay": row["lambda_per_day"],
            "lambdaEffective": row["lambda_effective"],
            "targetPrice": row["target_price"],
            "competitorsBelow": row["competitors_below"],
            "confidence": row["confidence"],
            "score": row["score"],
            "lowConfidence": bool(row["low_confidence"]),
            "passFilter": bool(row["pass_filter"]),
            "rejectReason": row["reject_reason"],
            "nextTierPtn": row["next_tier_ptn"],
            "nextTierPrice": row["next_tier_price"],
        }
        for row in rows
    }


def count_active_lots() -> int:
    """Сколько активных лотов в active_lots (status='active', до фильтра мозга)."""
    if not os.path.exists(ACTIVE_DB):
        return 0
    conn = sqlite3.connect(ACTIVE_DB)
    try:
        row = conn.execute("SELECT COUNT(*) FROM active_lots WHERE status = 'active'").fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def load_active_lots(mode: str = "scored") -> list[dict]:
    """mode='scored' (дефолт) — отдаёт все лоты со score-полями, отсортированные
    по score. Лоты, не прошедшие фильтр brain.py, НЕ скрываются — отсеивание
    выполняется на сайте. mode='all' — все лоты со score-полями (может быть
    None, если brain.py ещё не посчитал конкретный лот/сегмент)."""
    if not os.path.exists(ACTIVE_DB):
        return []

    conn = sqlite3.connect(ACTIVE_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT lot_key, item_id, amount, start_price, buyout_price, start_time, end_time,
                   qlt, ptn, stats_random, upgrade_bonus, bonus_json, extra_json, status,
                   first_seen_at, last_seen_at
            FROM active_lots
            ORDER BY end_time, item_id
            """
        ).fetchall()
    finally:
        conn.close()

    scores = load_scores()
    brain_ready = bool(scores)

    lots = []
    for row in rows:
        additional = {}
        if row["qlt"] is not None:
            additional["qlt"] = row["qlt"]
        if row["ptn"] is not None:
            additional["ptn"] = row["ptn"]
        if row["stats_random"] is not None:
            additional["stats_random"] = row["stats_random"]
        if row["upgrade_bonus"] is not None:
            additional["upgrade_bonus"] = row["upgrade_bonus"]

        bonus = json.loads(row["bonus_json"]) if row["bonus_json"] else []
        if bonus:
            additional["bonus_properties"] = bonus

        extra = json.loads(row["extra_json"]) if row["extra_json"] else {}
        if extra:
            additional.update(extra)

        score = scores.get(row["lot_key"])
        lot_status = row["status"] if "status" in row.keys() else "active"

        # Проданные лоты (status='gone') показываем всегда, без фильтра мозга
        if mode == "scored" and brain_ready and lot_status == "active" and not (score and score["passFilter"]):
            continue  # мозг отсекает: профит < 1 ₽ или нет fair value

        lots.append({
            "itemId": row["item_id"],
            "amount": row["amount"] or 1,
            "startPrice": row["start_price"] or 0,
            "buyoutPrice": row["buyout_price"] or 0,
            "startTime": row["start_time"],
            "endTime": row["end_time"],
            "firstSeenAt": row["first_seen_at"],
            "lastSeenAt": row["last_seen_at"],
            "additional": additional,
            "score": score,  # None, пока brain.py не посчитал этот сегмент/лот
            "status": lot_status,
        })

    if mode == "scored" and brain_ready:
        lots.sort(key=lambda l: (l["score"] or {}).get("score", 0) or 0, reverse=True)

    return lots


def load_history_for_item(item_id: str, hours: int = 24) -> list[dict]:
    if not os.path.exists(HISTORY_DB):
        return []

    table = sanitize_table_name(item_id)
    conn = sqlite3.connect(HISTORY_DB)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table,),
        ).fetchone()
        if not table_exists:
            return []

        cutoff = (time.time() - hours * 3600)
        cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff))
        rows = conn.execute(
            f'SELECT price, time, qlt, ptn, stats_random, upgrade_bonus, spawn_time FROM "{table}" WHERE time >= ? ORDER BY time',
            (cutoff_iso,),
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "price": row["price"],
            "time": row["time"],
            "qlt": row["qlt"],
            "ptn": row["ptn"],
            "stats_random": row["stats_random"],
            "upgrade_bonus": row["upgrade_bonus"],
            "spawn_time": row["spawn_time"],
        }
        for row in rows
    ]


def build_payload(mode: str = "scored") -> dict:
    return {
        "items": load_items(),
        "lots": load_active_lots(mode=mode),
        "total_lots": count_active_lots(),
        "brain_ready": os.path.exists(SCORES_DB),
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "last_active_run": get_last_active_run(),
    }


def load_brain_config() -> dict:
    if not os.path.exists(BRAIN_CONFIG_PATH):
        return {}
    with open(BRAIN_CONFIG_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_brain_config(cfg: dict) -> None:
    with open(BRAIN_CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)


def request_brain_recompute() -> None:
    """Просит brain.py пересчитать при следующем поллинге, не удаляя кэш —
    используется после сохранения конфига, чтобы новые пороги/формулы
    применились сразу, а не ждали следующего обновления active_lots."""
    if not os.path.exists(SCORES_DB):
        return  # brain.py ещё ни разу не запускался — само пересчитает при первом запуске
    conn = sqlite3.connect(SCORES_DB)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS brain_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT OR REPLACE INTO brain_state (key, value) VALUES ('force_recompute', '1')"
        )
        conn.commit()
    finally:
        conn.close()


def reset_brain_cache() -> None:
    """То же самое, что 'python brain.py --reset-cache', но вызывается прямо
    с сайта: чистит кэш fair value и просит brain.py пересчитать всё заново
    на следующем цикле опроса (см. brain.py: needs_recompute/force_recompute)."""
    conn = sqlite3.connect(SCORES_DB)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS brain_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute("DELETE FROM item_fair_value") if _table_exists(conn, "item_fair_value") else None
        conn.execute("DELETE FROM lot_scores") if _table_exists(conn, "lot_scores") else None
        conn.execute(
            "INSERT OR REPLACE INTO brain_state (key, value) VALUES ('force_recompute', '1')"
        )
        conn.commit()
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _query_params(parsed) -> dict[str, str]:
    if not parsed.query:
        return {}
    return dict(qp.split("=", 1) for qp in parsed.query.split("&") if "=" in qp)


class AuctionHandler(SimpleHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_sse(self) -> None:
        """Держит SSE-соединение открытым, регистрирует клиента."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        with _sse_lock:
            _sse_clients.append(self.wfile)
        try:
            while True:
                self.wfile.write(b": keep-alive\n\n")
                self.wfile.flush()
                time.sleep(20)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with _sse_lock:
                if self.wfile in _sse_clients:
                    _sse_clients.remove(self.wfile)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = _query_params(parsed)

        if parsed.path == "/api/events":
            self._handle_sse()
            return

        if parsed.path == "/api/auction-data":
            mode = params.get("mode", "scored")
            self._send_json(build_payload(mode=mode))
            return

        if parsed.path.startswith("/api/history/"):
            item_id = unquote(parsed.path[len("/api/history/"):])
            try:
                hours = int(params.get("hours", "24"))
            except ValueError:
                hours = 24
            records = load_history_for_item(item_id, hours=hours)
            self._send_json({"item_id": item_id, "hours": hours, "history": records})
            return

        if parsed.path == "/api/brain-config":
            self._send_json(load_brain_config())
            return

        if parsed.path == "/api/scan-progress":
            with _scan_progress_lock:
                self._send_json(dict(_scan_progress))
            return

        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/api/notify":
            # Коллектор уведомил: активные лоты обновлены — рассылаем SSE
            broadcast_update()
            self._send_json({"ok": True})
            return

        if parsed.path == "/api/scan-progress":
            # Коллектор шлёт лёгкий тик прогресса (total/fetched/computed) —
            # обновляем состояние в памяти и рассылаем НЕ 'data-updated'
            # (тот дёргает полную перезагрузку лотов), а отдельное лёгкое
            # событие 'scan-progress', чтобы полоска обновлялась плавно и
            # часто, без лишней нагрузки на фронт.
            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length) if length else b"{}"
            try:
                incoming = json.loads(body or b"{}")
            except json.JSONDecodeError:
                incoming = {}
            with _scan_progress_lock:
                for key in ("total", "fetched", "computed", "active"):
                    if key in incoming:
                        _scan_progress[key] = incoming[key]
                snapshot = dict(_scan_progress)
            broadcast_progress(snapshot)
            self._send_json({"ok": True})
            return

        if parsed.path == "/api/clear-gone":
            # Очистка проданных лотов (status='gone') — вызывается кнопкой с сайта
            conn = sqlite3.connect(ACTIVE_DB)
            try:
                deleted = conn.execute("DELETE FROM active_lots WHERE status = 'gone'").rowcount
                conn.commit()
            finally:
                conn.close()
            broadcast_update()
            self._send_json({"ok": True, "deleted": deleted})
            return

        if parsed.path == "/api/brain-config":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                cfg = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                self._send_json({"error": "invalid JSON"}, status=400)
                return
            save_brain_config(cfg)
            request_brain_recompute()
            self._send_json({"ok": True, "config": cfg})
            return

        if parsed.path == "/api/brain-config/reset-cache":
            reset_brain_cache()
            self._send_json({"ok": True})
            return

        self._send_json({"error": "not found"}, status=404)


def open_chrome():
    time.sleep(1)
    url = f"http://127.0.0.1:{PORT}/auctiontracker.html"

    try:
        subprocess.Popen([
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            url
        ])
    except FileNotFoundError:
        import webbrowser
        webbrowser.open(url)


def main() -> None:
    threading.Thread(target=open_chrome, daemon=True).start()
    print(f"Сервер запущен: http://127.0.0.1:{PORT}", flush=True)
    ThreadingHTTPServer(
        ("127.0.0.1", PORT),
        AuctionHandler
    ).serve_forever()


if __name__ == "__main__":
    main()