"""
"Мозг" аукциона STALZONE — аналитический демон поверх данных collector.py.

Не собирает данные сам, а обрабатывает то, что уже лежит в:
    auction_history.sqlite3  (история продаж, пишет job_history)
    auction_active.sqlite3   (снимок активных лотов, пишет job_active_lots)

Считает по сегменту (item_id, qlt[, ptn]) справедливую цену (fair value) и
ликвидность из истории, затем по каждому активному лоту — профит и score,
и складывает результат в отдельный кэш auction_scores.sqlite3, который
server.py джойнит к active_lots при отдаче на фронт.

Конфиг (пороги, формулы, фильтры) живёт в brain_config.json — файл читается
заново на каждый прогон, чтобы правки с сайта (через /api/brain-config
в server.py) применялись без перезапуска демона.

Пересчёт запускается двумя способами:

1. **Прямой вызов из collector.py** — после каждого успешного снимка активных
   лотов collector вызывает brain.recompute_all() в фоновом потоке, чтобы мозг
   начинал обработку мгновенно, без ожидания таймера.

2. **Polling (daemon-режим)** — если brain.py запущен как отдельный демон, он
   проверяет collector_state.last_active_run и флаг force_recompute каждые
   15 секунд. Это резервный механизм и обработка ручных изменений конфига
   с сайта (force_recompute).

Для защиты от одновременного выполнения (прямой вызов + daemon) используется
блокировка recompute_lock в brain_state (см. recompute_all).

Запуск:
    python brain.py              # daemon: следит за last_active_run, пересчитывает
    python brain.py --once       # один пересчёт (по текущим данным) и выход
    python brain.py --reset-cache  # сбросить кэш fair value и заставить
                                    # пересчитать всё с нуля на следующем цикле
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)  # type: ignore[attr-defined]

# ---- Пути ----

BASE_DIR = Path(__file__).resolve().parent

HISTORY_DB = BASE_DIR / "auction_history.sqlite3"
ACTIVE_DB = BASE_DIR / "auction_active.sqlite3"
SCORES_DB = BASE_DIR / "auction_scores.sqlite3"
CONFIG_PATH = BASE_DIR / "brain_config.json"
LOG_FILE = BASE_DIR / "brain.log"

POLL_INTERVAL_SEC = 15  # как часто проверять, не обновился ли активный снимок

# Защита от параллельного выполнения recompute_all внутри одного процесса
# (например, прямой вызов из collector.py + daemon_loop в одном процессе).
_recompute_lock = threading.Lock()

# ---- Logging ----

log = logging.getLogger("brain")


def setup_logging() -> None:
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)


# ===========================================================================
# Конфиг — дефолты на случай отсутствия файла. Редактируется живьём через
# server.py -> /api/brain-config. Комментарии к каждой настройке — здесь,
# т.к. JSON не поддерживает комментарии.
# ===========================================================================

DEFAULT_CONFIG = {
    "fair_value": {
        # --- базовые окна ---
        "window_sales": 50,          # сколько последних продаж брать в расчёт
        "window_days": 14,           # либо окно по времени (что раньше наступит)
        "decay_lambda": 0.15,        # скорость затухания веса старых продаж
        "min_sales_required": 10,    # меньше — сегмент считается ненадёжным
        # Точное сравнение по item_id+qlt+ptn (без бакетов/округления).
        # Выключить — сравнение будет по item_id+qlt без учёта заточки.
        "use_ptn_segmentation": True,
        # Что делать, если продаж в сегменте меньше min_sales_required:
        #   "hide"     — не считать вообще, лот не попадёт в выдачу
        #   "mark"     — посчитать, но пометить low_confidence=true
        #   "fallback" — откатиться на менее строгую группировку (без ptn,
        #                затем без qlt) и тоже пометить low_confidence=true
        "cold_start_mode": "mark",

        # --- 1.1 Перцентиль вместо медианы ---
        # Настраиваемый перцентиль (30-40) вместо 50-го (медианы).
        # Меньший процент = более консервативная (заниженная) оценка,
        # компенсирует систематическое смещение вверх из-за завышенных ask-цен.
        "fair_value_percentile": 35,

        # --- 1.2 Обрезка выбросов ---
        # Отсечь верхние N% самых дорогих продаж (по накопленному весу)
        # перед расчётом перцентиля. Аномально дорогие панические покупки
        # исключаются из выборки целиком.
        "outlier_trim_pct": 8,

        # --- Бонусные бракеты заточки (ptn) ---
        # Заточки дают игровой бонус на определённых порогах (5/10/15).
        # +4 и +5 — принципиально разные предметы по ценности, их нельзя
        # смешивать в один сегмент НИ ПРИ КАКОМ откате. +4 и +9 внутри
        # одного бракета сравнивать можно (оба ещё без следующего бонуса).
        "ptn_brackets": [[0, 4], [5, 9], [10, 14], [15, 15]],

        # Если за window_days (14) точному сегменту (qlt, ptn) не хватило
        # min_sales_required продаж — сначала расширяем ОКНО ПО ВРЕМЕНИ
        # (тот же точный ptn, просто смотрим дальше в прошлое), и только
        # если и это не помогло — расширяем СЕГМЕНТ до всего бракета ptn
        # (но не дальше границы бонуса). Порядок: время, потом сегмент.
        "extended_window_days": 90,

        # Хинт "соседний бонусный тир" — чисто информационная строка на
        # карточке лота для предметов рядом с порогом (например +9 рядом
        # с +10). НЕ входит в fair_value/score/профит — это подсказка
        # пользователю про потенциал доточки, а не пересчёт цены.
        "near_threshold_hint": {
            "enabled": True,
            "max_distance": 1,  # +9 к порогу +10 (расстояние 1) — покажется хинт
        },

        # --- 1.3 Контроль остывания рынка ---
        # Параллельно с основным fair value (длинное окно) считается короткое
        # (последние 3 дня / 15 продаж). Если короткое ниже длинного больше
        # чем на cooling_threshold_pct% — рынок остывает, сегмент помечается.
        "market_cooling": {
            "enabled": True,
            "short_window_days": 3,      # короткое окно по времени
            "short_window_sales": 15,    # короткое окно по числу продаж
            "cooling_threshold_pct": 10, # флаг, если короткое ниже длинного на X%
        },

        # --- 1.4 Blend узкого и широкого сегмента ---
        # Вместо жёсткого каскада fallback — взвешенная смесь:
        # fair_value = w * узкий_сегмент + (1-w) * широкий_сегмент.
        # w растёт с объёмом данных в узком сегменте, сглаживая переход
        # между "мало данных, берём широкий контекст" и "много данных,
        # доверяем точному сегменту".
        "blend_segments": {
            "enabled": True,
            "min_weight": 0.3,       # мин. вес узкого сегмента при малом объёме
        },
    },
    "liquidity": {
        "window_days": 7,           # окно для расчёта частоты продаж λ
        "target_hours": 12,         # целевой срок продажи лота (справочно)
        "confidence_level": 0.9,    # справочно, не используется в MVP-формуле score
    },
    "time_to_sell": {
        # --- Блок 2: time-to-sell по конкретной целевой цене ---
        # 2.2: λ(X) = λ_total * S(X), где S(X) — доля продаж с ценой ≥ X.
        # Выше цена → меньше S(X) → меньше эффективная лямбда → дольше ждать.
        "use_price_dependent_lambda": True,
        # 2.3: делить λ(X) на (1 + количество активных лотов дешевле X).
        # Покупатели сначала разбирают более дешёвые аналоги. Используется
        # ТОЛЬКО для расчёта времени продажи, не для самого fair value.
        "competitor_penalty": True,
        # От какой цены считать время продажи:
        #   "fair_value" — от справедливой цены (перепродажа по рынку)
        #   "buyout"     — от цены выкупа лота (перепродажа по цене покупки)
        "target_price_mode": "fair_value",
    },
    "entry_filter": {
        "min_absolute_profit": 5000,
        "min_absolute_profit_mode": "static",  # static | relative_to_median
        "min_percent_profit": 5,               # вторичный фильтр, %
        "max_price_lot": None,                 # null = без лимита (бюджет бесконечный)
        "min_price_lot": None,
    },
    "scoring": {
        "formula": "profit_per_hour",  # profit_per_hour | profit_only
        "confidence_penalty": True,
    },
    "orderbook": {
        "use_current_listings": True,
        "ignore_if_competitors_below": 3,  # пропустить, если N лотов сегмента дешевле fair_value уже висит
    },
    "fees": {
        "auction_tax_percent": 5,
    },
}


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return json.loads(json.dumps(DEFAULT_CONFIG))  # deep copy
    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        log.error("Не удалось прочитать %s (%s), использую дефолтный конфиг", CONFIG_PATH, e)
        return json.loads(json.dumps(DEFAULT_CONFIG))

    # неглубокий merge с дефолтом — чтобы новые поля конфига появлялись
    # у уже существующих пользовательских brain_config.json автоматически
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for section, values in raw.items():
        if isinstance(values, dict) and section in merged:
            if isinstance(merged[section], dict):
                # вложенные секции (market_cooling, blend_segments) тоже мержим
                for sub_key, sub_val in values.items():
                    if isinstance(sub_val, dict) and isinstance(merged[section].get(sub_key), dict):
                        merged[section][sub_key].update(sub_val)
                    else:
                        merged[section][sub_key] = sub_val
            else:
                merged[section].update(values)
        else:
            merged[section] = values
    return merged


def save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


# ===========================================================================
# Кэш-БД
# ===========================================================================

_SCORES_SCHEMA = """
CREATE TABLE IF NOT EXISTS item_fair_value (
    item_id         TEXT NOT NULL,
    qlt             INTEGER,
    ptn             INTEGER,
    fair_value      REAL,
    std_dev         REAL,
    sale_count      INTEGER,
    lambda_per_day  REAL,
    confidence      REAL,
    segmentation    TEXT,      -- 'exact' | 'bracket' | 'no_ptn' | 'blend' | 'none'
    low_confidence  INTEGER DEFAULT 0,
    market_cooling  INTEGER DEFAULT 0,   -- 1.3: рынок остывает (короткое < длинного)
    blend_weight    REAL,                -- 1.4: вес узкого сегмента в смеси
    extended_window INTEGER DEFAULT 0,   -- пришлось смотреть дальше 14 дней (тот же точный ptn)
    computed_at     TEXT,
    PRIMARY KEY (item_id, qlt, ptn)
);

CREATE TABLE IF NOT EXISTS lot_scores (
    lot_key                TEXT PRIMARY KEY,
    item_id                TEXT,
    qlt                    INTEGER,
    ptn                    INTEGER,
    buyout_price            INTEGER,
    fair_value              REAL,
    absolute_profit         REAL,
    percent_profit          REAL,
    expected_days_to_sell   REAL,
    lambda_per_day           REAL,
    lambda_effective         REAL,       -- 2.2+2.3: λ с учётом цены и конкурентов
    target_price             REAL,       -- 2.4: цена, от которой считали время продажи
    competitors_below        INTEGER,    -- 2.3: сколько активных лотов дешевле target_price
    confidence               REAL,
    score                    REAL,
    low_confidence           INTEGER DEFAULT 0,
    pass_filter              INTEGER DEFAULT 0,
    reject_reason            TEXT,
    fetch_run_id              INTEGER,
    computed_at                TEXT,
    next_tier_ptn             INTEGER,  -- хинт: соседний бонусный тир (например 10 для +9)
    next_tier_price           REAL      -- хинт: fair value этого соседнего тира, справочно
);
CREATE INDEX IF NOT EXISTS idx_scores_pass ON lot_scores(pass_filter, score);

CREATE TABLE IF NOT EXISTS brain_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def init_scores_db(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCORES_SCHEMA)
    conn.commit()

    # Миграция: добавляем новые колонки, если их ещё нет
    fv_columns = [row[1] for row in conn.execute("PRAGMA table_info(item_fair_value)")]
    if "market_cooling" not in fv_columns:
        conn.execute("ALTER TABLE item_fair_value ADD COLUMN market_cooling INTEGER DEFAULT 0")
    if "blend_weight" not in fv_columns:
        conn.execute("ALTER TABLE item_fair_value ADD COLUMN blend_weight REAL")
    if "extended_window" not in fv_columns:
        conn.execute("ALTER TABLE item_fair_value ADD COLUMN extended_window INTEGER DEFAULT 0")

    ls_columns = [row[1] for row in conn.execute("PRAGMA table_info(lot_scores)")]
    if "lambda_per_day" not in ls_columns:
        conn.execute("ALTER TABLE lot_scores ADD COLUMN lambda_per_day REAL")
    if "lambda_effective" not in ls_columns:
        conn.execute("ALTER TABLE lot_scores ADD COLUMN lambda_effective REAL")
    if "target_price" not in ls_columns:
        conn.execute("ALTER TABLE lot_scores ADD COLUMN target_price REAL")
    if "competitors_below" not in ls_columns:
        conn.execute("ALTER TABLE lot_scores ADD COLUMN competitors_below INTEGER")
    if "next_tier_ptn" not in ls_columns:
        conn.execute("ALTER TABLE lot_scores ADD COLUMN next_tier_ptn INTEGER")
    if "next_tier_price" not in ls_columns:
        conn.execute("ALTER TABLE lot_scores ADD COLUMN next_tier_price REAL")
    conn.commit()


def get_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM brain_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT OR REPLACE INTO brain_state (key, value) VALUES (?, ?)", (key, value))
    conn.commit()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ===========================================================================
# Чтение исходных данных
# ===========================================================================

def sanitize_table_name(item_id: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in item_id)
    return f"hist_{safe}"


def get_active_last_run(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT value FROM collector_state WHERE key='last_active_run'"
    ).fetchone()
    return row[0] if row else None


def load_item_ids() -> list[str]:
    items_path = BASE_DIR / "items.json"
    if not items_path.exists():
        return []
    return list(json.loads(items_path.read_text(encoding="utf-8")).keys())


def load_active_lots(conn: sqlite3.Connection, item_ids: list[str] | None = None) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    if item_ids:
        placeholders = ",".join("?" * len(item_ids))
        return conn.execute(
            "SELECT lot_key, item_id, qlt, ptn, buyout_price, fetch_run_id "
            f"FROM active_lots WHERE item_id IN ({placeholders})",
            item_ids,
        ).fetchall()
    return conn.execute(
        "SELECT lot_key, item_id, qlt, ptn, buyout_price, fetch_run_id "
        "FROM active_lots"
    ).fetchall()


# ===========================================================================
# Слой 1: fair value / ликвидность по истории (медленный, по сегментам)
# ===========================================================================

@dataclass
class FairValueResult:
    fair_value: float | None
    std_dev: float
    sale_count: int
    lambda_per_day: float
    confidence: float
    segmentation: str
    low_confidence: bool
    market_cooling: bool = False
    blend_weight: float | None = None
    # Взвешенные цены сегмента [(price, weight)] — нужны для S(X) в блоке 2
    weighted_prices: list[tuple[float, float]] = field(default_factory=list)
    # True, если точному ptn не хватило продаж за window_days и пришлось
    # смотреть дальше в прошлое (extended_window_days) тем же точным ptn
    extended_window: bool = False


def weighted_percentile(pairs: list[tuple[float, float]], percentile: float) -> float:
    """pairs = [(значение, вес), ...]. Возвращает значение на уровне percentile
    (0-100) накопленного веса. percentile=50 — медиана."""
    if not pairs:
        return 0.0
    ordered = sorted(pairs, key=lambda p: p[0])
    total = sum(w for _, w in ordered)
    if total <= 0:
        return ordered[len(ordered) // 2][0]
    target = total * percentile / 100.0
    cum = 0.0
    for value, weight in ordered:
        cum += weight
        if cum >= target:
            return value
    return ordered[-1][0]


def trim_outliers(pairs: list[tuple[float, float]], trim_pct: float) -> list[tuple[float, float]]:
    """1.2: отсечь верхние trim_pct% самых дорогих продаж (по накопленному весу).
    Аномально дорогие панические покупки исключаются из выборки целиком."""
    if trim_pct <= 0 or not pairs:
        return pairs
    ordered = sorted(pairs, key=lambda p: p[0])
    total = sum(w for _, w in ordered)
    if total <= 0:
        return pairs
    cutoff = total * (1.0 - trim_pct / 100.0)
    cum = 0.0
    kept: list[tuple[float, float]] = []
    for value, weight in ordered:
        cum += weight
        if cum > cutoff:
            break
        kept.append((value, weight))
    return kept or pairs  # не возвращаем пустой список


def get_ptn_bracket(ptn: int | None, brackets: list[list[int]]) -> tuple[int, int] | None:
    """Возвращает (low, high) бракета, в который попадает ptn, либо None
    если ptn отсутствует (безточный предмет не участвует в бракетах)."""
    if ptn is None:
        return None
    for low, high in brackets:
        if low <= ptn <= high:
            return (low, high)
    return None


def _query_segment_sales_bracket(
    hconn: sqlite3.Connection, table: str, qlt: int | None,
    bracket: tuple[int, int], window_days: int, limit: int,
) -> list[tuple[float, str]]:
    """Продажи внутри одного бонусного бракета ptn (никогда не пересекает
    границу бонуса), но по всем ptn внутри бракета."""
    cutoff = time.time() - window_days * 86400
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff))
    low, high = bracket

    where = ["time >= ?", "ptn >= ?", "ptn <= ?"]
    params: list = [cutoff_iso, low, high]
    if qlt is not None:
        where.append("qlt = ?")
        params.append(qlt)
    else:
        where.append("qlt IS NULL")

    sql = f'SELECT price, time FROM "{table}" WHERE {" AND ".join(where)} ORDER BY time DESC LIMIT ?'
    params.append(limit)
    try:
        return [(row[0], row[1]) for row in hconn.execute(sql, params).fetchall()]
    except sqlite3.OperationalError:
        return []


def find_next_threshold_ptn(
    ptn: int | None, brackets: list[list[int]], max_distance: int,
) -> int | None:
    """Если ptn стоит близко (<= max_distance) к следующему бонусному порогу —
    возвращает значение ptn этого порога (например 9 -> 10). Иначе None.
    Только "вверх" (к следующему бонусу), не "вниз"."""
    if ptn is None:
        return None
    bracket = get_ptn_bracket(ptn, brackets)
    if bracket is None:
        return None
    _, high = bracket
    next_ptn = high + 1
    if next_ptn - ptn > max_distance:
        return None
    if get_ptn_bracket(next_ptn, brackets) is None:
        return None  # уже максимальный бракет, дальше некуда
    return next_ptn


def _query_segment_sales(
    hconn: sqlite3.Connection, table: str, qlt: int | None, ptn: int | None,
    use_ptn: bool, window_days: int, limit: int,
) -> list[tuple[float, str]]:
    cutoff = time.time() - window_days * 86400
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff))

    where = ["time >= ?"]
    params: list = [cutoff_iso]
    if qlt is not None:
        where.append("qlt = ?")
        params.append(qlt)
    else:
        where.append("qlt IS NULL")
    if use_ptn:
        if ptn is not None:
            where.append("ptn = ?")
            params.append(ptn)
        else:
            where.append("ptn IS NULL")

    sql = f'SELECT price, time FROM "{table}" WHERE {" AND ".join(where)} ORDER BY time DESC LIMIT ?'
    params.append(limit)
    try:
        return [(row[0], row[1]) for row in hconn.execute(sql, params).fetchall()]
    except sqlite3.OperationalError:
        return []


def _build_weighted_pairs(rows: list[tuple[float, str]], decay_lambda: float) -> list[tuple[float, float]]:
    """Превращает (price, time) в (price, weight) с экспоненциальным затуханием."""
    now = time.time()
    weighted_pairs = []
    for price, ts in rows:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            days_ago = max(0.0, (now - dt.timestamp()) / 86400)
        except ValueError:
            days_ago = 0.0
        weight = math.exp(-decay_lambda * days_ago)
        weighted_pairs.append((price, weight))
    return weighted_pairs


def _compute_stats(prices: list[float], fair_value: float) -> tuple[float, float]:
    """Возвращает (std_dev, confidence)."""
    if not prices:
        return 0.0, 0.0
    mean_price = sum(prices) / len(prices)
    variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
    std_dev = math.sqrt(variance)
    confidence = 1.0 / (1.0 + (std_dev / fair_value if fair_value else 1.0))
    return std_dev, confidence


def _compute_fair_value_from_rows(
    rows: list[tuple[float, str]], fv_cfg: dict,
) -> tuple[float | None, float, float]:
    """Считает fair value (перцентиль + обрезка выбросов) из строк продаж.
    Возвращает (fair_value, std_dev, confidence)."""
    if not rows:
        return None, 0.0, 0.0

    weighted_pairs = _build_weighted_pairs(rows, fv_cfg["decay_lambda"])
    # 1.2: обрезка выбросов
    trimmed = trim_outliers(weighted_pairs, fv_cfg.get("outlier_trim_pct", 0))
    # 1.1: перцентиль вместо медианы
    percentile = fv_cfg.get("fair_value_percentile", 50)
    fair_value = weighted_percentile(trimmed, percentile)

    prices = [p for p, _ in trimmed]
    std_dev, confidence = _compute_stats(prices, fair_value)
    return fair_value, std_dev, confidence


def compute_fair_value(
    hconn: sqlite3.Connection, item_id: str, qlt: int | None, ptn: int | None, cfg: dict,
) -> FairValueResult:
    fv_cfg = cfg["fair_value"]
    liq_cfg = cfg["liquidity"]
    table = sanitize_table_name(item_id)
    use_ptn = bool(fv_cfg["use_ptn_segmentation"])
    cold_mode = fv_cfg["cold_start_mode"]
    min_required = fv_cfg["min_sales_required"]
    blend_cfg = fv_cfg.get("blend_segments", {})
    cooling_cfg = fv_cfg.get("market_cooling", {})
    brackets = fv_cfg.get("ptn_brackets", [[0, 4], [5, 9], [10, 14], [15, 15]])
    extended_days = fv_cfg.get("extended_window_days", fv_cfg["window_days"])

    # --- Точный сегмент (item_id, qlt, ptn) с трёхшаговым фолбэком ---
    # ВАЖНО: фолбэк никогда не пересекает границу бонусного бракета ptn
    # (+4 и +5 — разные предметы по ценности, их нельзя смешивать).
    # Шаг 1: точный ptn, обычное окно (window_days).
    # Шаг 2: точный ptn, расширенное окно (extended_window_days) — сначала
    #        расширяем время, а не сегмент.
    # Шаг 3: весь бракет ptn (но не дальше его границ), расширенное окно.
    narrow_rows: list[tuple[float, str]] = []
    used_extended_window = False
    used_bracket = False

    if use_ptn:
        narrow_rows = _query_segment_sales(
            hconn, table, qlt, ptn, True, fv_cfg["window_days"], fv_cfg["window_sales"],
        )
        # Расширяем окно по ВРЕМЕНИ, если продаж НЕДОСТАТОЧНО (не только если
        # их вообще нет) — сначала пробуем найти больше того же точного ptn
        # дальше в прошлом, прежде чем расширять сегмент.
        if len(narrow_rows) < min_required and extended_days > fv_cfg["window_days"]:
            extended_rows = _query_segment_sales(
                hconn, table, qlt, ptn, True, extended_days, fv_cfg["window_sales"],
            )
            if len(extended_rows) > len(narrow_rows):
                narrow_rows = extended_rows
                used_extended_window = True
        if len(narrow_rows) < min_required:
            bracket = get_ptn_bracket(ptn, brackets)
            if bracket is not None:
                bracket_rows = _query_segment_sales_bracket(
                    hconn, table, qlt, bracket, extended_days, fv_cfg["window_sales"],
                )
                # Берём весь бракет только если это реально даёт больше данных
                if len(bracket_rows) > len(narrow_rows):
                    narrow_rows = bracket_rows
                    used_bracket = True
    else:
        narrow_rows = _query_segment_sales(
            hconn, table, qlt, None, False, fv_cfg["window_days"], fv_cfg["window_sales"],
        )

    # Широкий сегмент для blend — тоже больше не пересекает границу бракета:
    # это "весь бракет" данного ptn, а не item_id без qlt/ptn вообще.
    bracket_for_wide = get_ptn_bracket(ptn, brackets) if use_ptn else None
    if bracket_for_wide is not None:
        wide_rows = _query_segment_sales_bracket(
            hconn, table, qlt, bracket_for_wide, fv_cfg["window_days"], fv_cfg["window_sales"],
        )
    else:
        wide_rows = _query_segment_sales(
            hconn, table, qlt, None, False, fv_cfg["window_days"], fv_cfg["window_sales"],
        )

    blend_enabled = bool(blend_cfg.get("enabled", True))
    min_weight = float(blend_cfg.get("min_weight", 0.3))

    # Вес узкого сегмента растёт с объёмом данных
    narrow_count = len(narrow_rows)
    wide_count = len(wide_rows)
    if blend_enabled and wide_count > 0:
        # w от 0.3 (мало данных) до 1.0 (много данных)
        w = min(1.0, min_weight + (1.0 - min_weight) * (narrow_count / max(min_required, 1)))
        w = max(min_weight, min(1.0, w))
    else:
        w = 1.0 if narrow_count > 0 else 0.0

    # Считаем fair value для узкого и широкого сегмента
    narrow_fv, narrow_std, narrow_conf = _compute_fair_value_from_rows(narrow_rows, fv_cfg)
    wide_fv, wide_std, wide_conf = _compute_fair_value_from_rows(wide_rows, fv_cfg)

    if narrow_fv is None and wide_fv is None:
        return FairValueResult(None, 0.0, 0, 0.0, 0.0, "none", True)

    # narrow_mode описывает, ЧЕМ реально наполнен narrow_rows — нужно для
    # честного пере-запроса окна ликвидности/cooling той же группировкой.
    if not use_ptn:
        narrow_mode = "no_ptn"
    elif used_bracket:
        narrow_mode = "bracket"
    else:
        narrow_mode = "exact"

    # Взвешенные цены для S(X): берём из узкого сегмента (основного)
    seg_weighted_prices = _build_weighted_pairs(narrow_rows or wide_rows, fv_cfg["decay_lambda"])

    # Смешиваем
    if narrow_fv is not None and wide_fv is not None and blend_enabled:
        fair_value = w * narrow_fv + (1 - w) * wide_fv
        segmentation = "blend"
        sale_count = narrow_count
        std_dev = narrow_std if narrow_std > 0 else wide_std
        confidence = narrow_conf if narrow_conf > 0 else wide_conf
        reseg_mode = narrow_mode
    elif narrow_fv is not None:
        fair_value = narrow_fv
        segmentation = narrow_mode
        sale_count = narrow_count
        std_dev = narrow_std
        confidence = narrow_conf
        reseg_mode = narrow_mode
    else:
        fair_value = wide_fv
        segmentation = "bracket" if use_ptn else "no_ptn"
        sale_count = wide_count
        std_dev = wide_std
        confidence = wide_conf
        seg_weighted_prices = _build_weighted_pairs(wide_rows, fv_cfg["decay_lambda"])
        reseg_mode = "bracket" if use_ptn else "no_ptn"

    # low_confidence: мало продаж ИЛИ пришлось откатиться на весь бракет
    # (это уже не точный ptn, честно предупреждаем пользователя)
    low_confidence = sale_count < min_required or reseg_mode == "bracket"

    if fair_value is None or (low_confidence and cold_mode == "hide"):
        return FairValueResult(None, 0.0, sale_count, 0.0, 0.0, segmentation, True)

    def _reseg_sales(window_days: int, limit: int) -> list[tuple[float, str]]:
        """Пере-запрос продаж той же группировкой, что победила в narrow/wide
        (exact ptn | весь бракет ptn | без ptn), для cooling и ликвидности."""
        if reseg_mode == "exact":
            return _query_segment_sales(hconn, table, qlt, ptn, True, window_days, limit)
        if reseg_mode == "bracket":
            bracket = get_ptn_bracket(ptn, brackets)
            if bracket is not None:
                return _query_segment_sales_bracket(hconn, table, qlt, bracket, window_days, limit)
        return _query_segment_sales(hconn, table, qlt, None, False, window_days, limit)

    # --- 1.3: Контроль остывания рынка ---
    market_cooling = False
    if cooling_cfg.get("enabled", True):
        short_rows = _reseg_sales(
            cooling_cfg.get("short_window_days", 3),
            cooling_cfg.get("short_window_sales", 15),
        )
        if short_rows:
            short_fv, _, _ = _compute_fair_value_from_rows(short_rows, fv_cfg)
            if short_fv is not None and fair_value:
                drop_pct = (fair_value - short_fv) / fair_value * 100.0
                if drop_pct > cooling_cfg.get("cooling_threshold_pct", 10):
                    market_cooling = True

    # ликвидность считается отдельным окном (liquidity.window_days), той же группировкой
    liq_rows = _reseg_sales(liq_cfg["window_days"], limit=10_000)
    lambda_per_day = len(liq_rows) / max(1, liq_cfg["window_days"])

    return FairValueResult(
        fair_value=fair_value,
        std_dev=std_dev,
        sale_count=sale_count,
        lambda_per_day=lambda_per_day,
        confidence=confidence,
        segmentation=segmentation,
        low_confidence=low_confidence,
        market_cooling=market_cooling,
        blend_weight=w if segmentation == "blend" else None,
        weighted_prices=seg_weighted_prices,
        extended_window=used_extended_window,
    )


def recompute_fair_values(
    hconn: sqlite3.Connection, sconn: sqlite3.Connection, cfg: dict,
    item_ids: list[str] | None = None,
) -> dict:
    """Считает fair value по сегментам, встречающимся в активных лотах, и
    складывает в item_fair_value. Возвращает {(item_id,qlt,ptn): FairValueResult}.

    Если item_ids задан — считаются только сегменты этих айтемов (частичный
    пересчёт после опроса конкретных артефактов), остальные не трогаются."""
    active_conn = sqlite3.connect(ACTIVE_DB, timeout=30)
    active_conn.row_factory = sqlite3.Row
    if item_ids:
        placeholders = ",".join("?" * len(item_ids))
        segments = active_conn.execute(
            f"SELECT DISTINCT item_id, qlt, ptn FROM active_lots WHERE item_id IN ({placeholders})",
            item_ids,
        ).fetchall()
    else:
        segments = active_conn.execute(
            "SELECT DISTINCT item_id, qlt, ptn FROM active_lots"
        ).fetchall()
    active_conn.close()

    results: dict[tuple, FairValueResult] = {}
    now_iso = utc_now_iso()
    for row in segments:
        item_id, qlt, ptn = row["item_id"], row["qlt"], row["ptn"]
        res = compute_fair_value(hconn, item_id, qlt, ptn, cfg)
        results[(item_id, qlt, ptn)] = res
        sconn.execute(
            """
            INSERT INTO item_fair_value
                (item_id, qlt, ptn, fair_value, std_dev, sale_count, lambda_per_day,
                 confidence, segmentation, low_confidence, market_cooling, blend_weight,
                 extended_window, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id, qlt, ptn) DO UPDATE SET
                fair_value=excluded.fair_value, std_dev=excluded.std_dev,
                sale_count=excluded.sale_count, lambda_per_day=excluded.lambda_per_day,
                confidence=excluded.confidence, segmentation=excluded.segmentation,
                low_confidence=excluded.low_confidence, market_cooling=excluded.market_cooling,
                blend_weight=excluded.blend_weight, extended_window=excluded.extended_window,
                computed_at=excluded.computed_at
            """,
            (item_id, qlt, ptn, res.fair_value, res.std_dev, res.sale_count,
             res.lambda_per_day, res.confidence, res.segmentation,
             int(res.low_confidence), int(res.market_cooling), res.blend_weight,
             int(res.extended_window), now_iso),
        )
    sconn.commit()
    log.info("fair value пересчитан для %d сегментов", len(segments))

    # --- Хинт "соседний бонусный тир" ---
    # Для лотов рядом с порогом (например +9 при пороге +10) отдельно
    # считаем fair value соседнего тира. Чисто информационно, в основной
    # fair_value/score НЕ подмешивается — см. compute_lot_score.
    hint_cfg = fv_cfg_top(cfg).get("near_threshold_hint", {})
    hints: dict[tuple, dict] = {}
    if hint_cfg.get("enabled", True):
        max_distance = hint_cfg.get("max_distance", 1)
        brackets = fv_cfg_top(cfg).get("ptn_brackets", [[0, 4], [5, 9], [10, 14], [15, 15]])
        for row in segments:
            item_id, qlt, ptn = row["item_id"], row["qlt"], row["ptn"]
            next_ptn = find_next_threshold_ptn(ptn, brackets, max_distance)
            if next_ptn is None:
                continue
            next_key = (item_id, qlt, next_ptn)
            if next_key in results:
                next_fv = results[next_key].fair_value
            else:
                next_fv = compute_fair_value(hconn, item_id, qlt, next_ptn, cfg).fair_value
            if next_fv is not None:
                hints[(item_id, qlt, ptn)] = {"next_ptn": next_ptn, "next_price": next_fv}

    return results, hints


def fv_cfg_top(cfg: dict) -> dict:
    """Короткий помощник — секция fair_value конфига."""
    return cfg["fair_value"]


# ===========================================================================
# Блок 2: time-to-sell по конкретной целевой цене
# ===========================================================================

def survival_probability(target_price: float, weighted_prices: list[tuple[float, float]]) -> float:
    """2.1: S(X) — доля продаж с ценой ≥ X (по накопленному весу).
    Сортируем от самой дорогой к дешёвой, накапливаем вес."""
    if not weighted_prices:
        return 0.0
    total = sum(w for _, w in weighted_prices)
    if total <= 0:
        return 0.0
    ordered = sorted(weighted_prices, key=lambda p: p[0], reverse=True)
    cum = 0.0
    for price, weight in ordered:
        if price >= target_price:
            cum += weight
        else:
            break
    return cum / total


def lambda_at_price(lambda_total: float, s_x: float) -> float:
    """2.2: λ(X) = λ_total * S(X)."""
    return lambda_total * s_x


def lambda_effective(lambda_x: float, competitors_below: int) -> float:
    """2.3: делить λ(X) на (1 + количество активных лотов дешевле X)."""
    return lambda_x / (1.0 + competitors_below)


def expected_days_to_sell(lambda_eff: float) -> float:
    """2.4: ожидаемое время продажи (дни) по эффективной лямбде."""
    if lambda_eff <= 0:
        return float("inf")
    return 1.0 / lambda_eff


# ===========================================================================
# Слой 2: score по активным лотам (быстрый, арифметика над кэшем)
# ===========================================================================

def compute_lot_score(
    lot: sqlite3.Row, fv: FairValueResult, orderbook_competitors: int, cfg: dict,
    hint: dict | None = None,
) -> dict:
    entry_cfg = cfg["entry_filter"]
    fees_cfg = cfg["fees"]
    ob_cfg = cfg["orderbook"]
    scoring_cfg = cfg["scoring"]
    tts_cfg = cfg.get("time_to_sell", {})

    lot_price = lot["buyout_price"] or 0
    reject_reason = None

    if fv.fair_value is None:
        reject_reason = "no_fair_value"
    elif fv.low_confidence and cfg["fair_value"]["cold_start_mode"] == "hide":
        reject_reason = "cold_start_hidden"

    tax = fees_cfg["auction_tax_percent"] / 100.0
    fair_value = fv.fair_value or 0.0
    absolute_profit = fair_value * (1 - tax) - lot_price
    percent_profit = (absolute_profit / lot_price * 100.0) if lot_price else 0.0

    if reject_reason is None:
        if entry_cfg["min_price_lot"] is not None and lot_price < entry_cfg["min_price_lot"]:
            reject_reason = "price_too_low"
        elif entry_cfg["max_price_lot"] is not None and lot_price > entry_cfg["max_price_lot"]:
            reject_reason = "price_too_high"
        elif absolute_profit < entry_cfg["min_absolute_profit"]:
            reject_reason = "profit_below_threshold"
        elif percent_profit < entry_cfg["min_percent_profit"]:
            reject_reason = "percent_profit_below_threshold"
        elif ob_cfg["use_current_listings"] and orderbook_competitors >= ob_cfg["ignore_if_competitors_below"]:
            reject_reason = "orderbook_saturated"

    # --- Блок 2: time-to-sell от целевой цены ---
    # Целевая цена перепродажи (target_price)
    target_price_mode = tts_cfg.get("target_price_mode", "fair_value")
    if target_price_mode == "buyout":
        target_price = lot_price
    else:
        target_price = fair_value

    # Конкуренты дешевле target_price (для time-to-sell, не для fair value)
    competitors_below = orderbook_competitors  # уже посчитано как "дешевле fair_value"

    # Базовая лямбда сегмента
    lambda_total = fv.lambda_per_day

    # 2.2: λ(X) = λ_total * S(X), где S(X) — доля продаж с ценой ≥ X
    if tts_cfg.get("use_price_dependent_lambda", True):
        s_x = survival_probability(target_price, fv.weighted_prices)
        lambda_x = lambda_at_price(lambda_total, s_x)
    else:
        lambda_x = lambda_total

    # 2.3: поправка на конкурентов
    if tts_cfg.get("competitor_penalty", True):
        lambda_eff = lambda_effective(lambda_x, competitors_below)
    else:
        lambda_eff = lambda_x

    # 2.4: ожидаемое время продажи
    expected_days = expected_days_to_sell(lambda_eff)

    if scoring_cfg["formula"] == "profit_only":
        raw_score = absolute_profit
    else:  # profit_per_hour
        hours = expected_days * 24
        raw_score = (absolute_profit / hours) if math.isfinite(hours) and hours > 0 else 0.0

    if scoring_cfg["confidence_penalty"]:
        raw_score *= fv.confidence

    score = raw_score if reject_reason is None else 0.0
    pass_filter = reject_reason is None

    return {
        "lot_key": lot["lot_key"],
        "item_id": lot["item_id"],
        "qlt": lot["qlt"],
        "ptn": lot["ptn"],
        "buyout_price": lot_price,
        "fair_value": fv.fair_value,
        "absolute_profit": absolute_profit,
        "percent_profit": percent_profit,
        "expected_days_to_sell": None if math.isinf(expected_days) else expected_days,
        "lambda_per_day": lambda_total,
        "lambda_effective": lambda_eff,
        "target_price": target_price,
        "competitors_below": competitors_below,
        "confidence": fv.confidence,
        "score": score,
        "low_confidence": int(fv.low_confidence),
        "pass_filter": int(pass_filter),
        "reject_reason": reject_reason,
        "fetch_run_id": lot["fetch_run_id"],
        "computed_at": utc_now_iso(),
        # Хинт "соседний бонусный тир" — чисто информационно, не участвует
        # ни в absolute_profit, ни в score, ни в expected_days_to_sell выше.
        "next_tier_ptn": hint.get("next_ptn") if hint else None,
        "next_tier_price": hint.get("next_price") if hint else None,
    }


def recompute_lot_scores(
    sconn: sqlite3.Connection, fair_values: dict, hints: dict, cfg: dict,
    item_ids: list[str] | None = None,
) -> int:
    active_conn = sqlite3.connect(ACTIVE_DB, timeout=30)
    active_conn.row_factory = sqlite3.Row
    lots = load_active_lots(active_conn, item_ids)
    active_conn.close()

    # для orderbook-поправки: сколько активных лотов в каждом сегменте
    # стоят дешевле соответствующего fair_value (конкуренция по цене)
    segment_lots: dict[tuple, list[int]] = {}
    for lot in lots:
        key = (lot["item_id"], lot["qlt"], lot["ptn"])
        segment_lots.setdefault(key, []).append(lot["buyout_price"] or 0)

    rows_to_write = []
    for lot in lots:
        key = (lot["item_id"], lot["qlt"], lot["ptn"])
        fv = fair_values.get(key)
        if fv is None:
            continue
        fair_value = fv.fair_value or 0.0
        competitors = sum(
            1 for p in segment_lots.get(key, [])
            if p and p < fair_value and p != lot["buyout_price"]
        )
        hint = hints.get(key)
        rows_to_write.append(compute_lot_score(lot, fv, competitors, cfg, hint))

    if item_ids:
        # частичный пересчёт: трогаем только lot_scores этих айтемов,
        # остальные (посчитанные ранее) не удаляем. DELETE+INSERT ниже —
        # один implicit-транзакшн sqlite3 до commit(), как и раньше.
        placeholders = ",".join("?" * len(item_ids))
        sconn.execute(f"DELETE FROM lot_scores WHERE item_id IN ({placeholders})", item_ids)
    else:
        sconn.execute("DELETE FROM lot_scores")  # полный снимок пересчитывается целиком
    sconn.executemany(
        """
        INSERT INTO lot_scores
            (lot_key, item_id, qlt, ptn, buyout_price, fair_value, absolute_profit,
             percent_profit, expected_days_to_sell, lambda_per_day, lambda_effective,
             target_price, competitors_below, confidence, score, low_confidence,
             pass_filter, reject_reason, fetch_run_id, computed_at,
             next_tier_ptn, next_tier_price)
        VALUES (:lot_key, :item_id, :qlt, :ptn, :buyout_price, :fair_value, :absolute_profit,
                :percent_profit, :expected_days_to_sell, :lambda_per_day, :lambda_effective,
                :target_price, :competitors_below, :confidence, :score, :low_confidence,
                :pass_filter, :reject_reason, :fetch_run_id, :computed_at,
                :next_tier_ptn, :next_tier_price)
        """,
        rows_to_write,
    )
    sconn.commit()
    passed = sum(r["pass_filter"] for r in rows_to_write)
    log.info("score посчитан для %d лотов, прошло фильтр=%d", len(rows_to_write), passed)
    return len(rows_to_write)


# ===========================================================================
# Основной цикл пересчёта
# ===========================================================================

def recompute_all() -> None:
    # Threading lock — защита от параллельного вызова внутри одного процесса
    # (прямой вызов из collector.py + daemon_loop в одном процессе).
    if not _recompute_lock.acquire(blocking=False):
        log.info("recompute_all: уже выполняется в этом процессе, пропуск")
        return

    try:
        cfg = load_config()

        if not ACTIVE_DB.exists():
            log.warning("Нет %s, пересчёт пропущен", ACTIVE_DB)
            return
        if not HISTORY_DB.exists():
            log.warning("Нет %s, пересчёт пропущен", HISTORY_DB)
            return

        sconn = sqlite3.connect(SCORES_DB, timeout=30)
        sconn.execute("PRAGMA journal_mode=WAL;")
        init_scores_db(sconn)

        # DB-based lock — защита от параллельного вызова из разных процессов
        # (collector.py поток + brain.py daemon в отдельном процессе).
        # brain_state имеет PRIMARY KEY на key, поэтому INSERT атомарен.
        existing_lock = sconn.execute(
            "SELECT value FROM brain_state WHERE key='recompute_lock'"
        ).fetchone()
        if existing_lock:
            try:
                lock_time = datetime.fromisoformat(existing_lock[0].replace("Z", "+00:00"))
                age_sec = (datetime.now(timezone.utc) - lock_time).total_seconds()
                if age_sec < 1800:  # 30 минут — считается, что пересчёт ещё идёт
                    log.info("recompute_all: пересчёт уже идёт в другом процессе (%.0fс), пропуск", age_sec)
                    sconn.close()
                    return
                log.warning("recompute_all: найден устаревший лок (%.0fс), продолжаю", age_sec)
            except (ValueError, AttributeError):
                pass  # невалидный timestamp — ставим новый лок

        set_state(sconn, "recompute_lock", utc_now_iso())

        hconn = sqlite3.connect(HISTORY_DB, timeout=30)

        started = time.monotonic()
        try:
            fair_values, hints = recompute_fair_values(hconn, sconn, cfg)
            recompute_lot_scores(sconn, fair_values, hints, cfg)

            active_conn = sqlite3.connect(ACTIVE_DB, timeout=30)
            last_run = get_active_last_run(active_conn)
            active_conn.close()
            if last_run:
                set_state(sconn, "last_processed_active_run", last_run)
            set_state(sconn, "force_recompute", "0")
            set_state(sconn, "last_recompute_at", utc_now_iso())

            log.info("=== пересчёт готов за %.1fс ===", time.monotonic() - started)
        finally:
            sconn.execute("DELETE FROM brain_state WHERE key='recompute_lock'")
            sconn.commit()
            hconn.close()
            sconn.close()
    finally:
        _recompute_lock.release()


def recompute_for_items(item_ids: list[str]) -> bool:
    """Частичный пересчёт: только сегменты/лоты указанных айтемов, без
    глобального DELETE/пересчёта всей таблицы lot_scores. Используется
    коллектором сразу по готовности каждого айтема (см. mark_dirty в
    collector.py), а не только раз в POLL_INTERVAL_SEC.

    Возвращает False (и ничего не делает), если пересчёт уже идёт — в этом
    или в другом процессе. Вызывающий код (brain_worker_loop) должен вернуть
    item_ids обратно в очередь и попробовать на следующем цикле дебаунса —
    ничего не теряется, просто откладывается на секунду-другую."""
    if not item_ids:
        return True

    if not _recompute_lock.acquire(blocking=False):
        log.debug("recompute_for_items: пересчёт уже идёт в этом процессе, отложено")
        return False

    try:
        cfg = load_config()

        if not ACTIVE_DB.exists() or not HISTORY_DB.exists():
            return False

        sconn = sqlite3.connect(SCORES_DB, timeout=30)
        sconn.execute("PRAGMA journal_mode=WAL;")
        init_scores_db(sconn)

        # Тот же межпроцессный DB-лок, что и у recompute_all — частичный и
        # полный пересчёт никогда не должны писать в SCORES_DB одновременно.
        existing_lock = sconn.execute(
            "SELECT value FROM brain_state WHERE key='recompute_lock'"
        ).fetchone()
        if existing_lock:
            try:
                lock_time = datetime.fromisoformat(existing_lock[0].replace("Z", "+00:00"))
                age_sec = (datetime.now(timezone.utc) - lock_time).total_seconds()
                if age_sec < 1800:
                    log.debug("recompute_for_items: занято другим процессом (%.0fс), отложено", age_sec)
                    sconn.close()
                    return False
                log.warning("recompute_for_items: найден устаревший лок (%.0fс), продолжаю", age_sec)
            except (ValueError, AttributeError):
                pass

        set_state(sconn, "recompute_lock", utc_now_iso())
        hconn = sqlite3.connect(HISTORY_DB, timeout=30)

        started = time.monotonic()
        try:
            fair_values, hints = recompute_fair_values(hconn, sconn, cfg, item_ids)
            recompute_lot_scores(sconn, fair_values, hints, cfg, item_ids)
            set_state(sconn, "last_recompute_at", utc_now_iso())
            log.info(
                "=== частичный пересчёт (%d айтемов) готов за %.2fс ===",
                len(item_ids), time.monotonic() - started,
            )
        finally:
            sconn.execute("DELETE FROM brain_state WHERE key='recompute_lock'")
            sconn.commit()
            hconn.close()
            sconn.close()
        return True
    finally:
        _recompute_lock.release()


def needs_recompute() -> bool:
    if not ACTIVE_DB.exists():
        return False

    active_conn = sqlite3.connect(ACTIVE_DB, timeout=30)
    last_active_run = get_active_last_run(active_conn)
    active_conn.close()
    if last_active_run is None:
        return False

    sconn = sqlite3.connect(SCORES_DB, timeout=30)
    init_scores_db(sconn)
    last_processed = get_state(sconn, "last_processed_active_run")
    force = get_state(sconn, "force_recompute") == "1"
    sconn.close()

    return force or last_processed != last_active_run


def reset_cache() -> None:
    """Вызывается вручную (--reset-cache) либо через server.py при нажатии
    кнопки 'сбросить кэш fair value' на сайте (после патча экономики и т.п.)."""
    sconn = sqlite3.connect(SCORES_DB, timeout=30)
    init_scores_db(sconn)
    sconn.execute("DELETE FROM item_fair_value")
    sconn.execute("DELETE FROM lot_scores")
    set_state(sconn, "force_recompute", "1")
    sconn.close()
    log.info("Кэш fair value сброшен, пересчёт будет запущен на следующем цикле")


# ===========================================================================
# CLI / daemon
# ===========================================================================

_shutdown_requested = False


def _handle_signal(signum, _frame) -> None:
    global _shutdown_requested
    log.info("Получен сигнал %s, завершение...", signum)
    _shutdown_requested = True


def daemon_loop() -> None:
    log.info("Brain daemon запущен (poll каждые %dс)", POLL_INTERVAL_SEC)
    while not _shutdown_requested:
        try:
            if needs_recompute():
                recompute_all()
        except Exception:
            log.exception("Ошибка в цикле пересчёта")
        for _ in range(POLL_INTERVAL_SEC):
            if _shutdown_requested:
                break
            time.sleep(1)
    log.info("Brain daemon остановлен")


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="STALZONE auction brain")
    parser.add_argument("--once", action="store_true", help="один пересчёт и выход")
    parser.add_argument("--reset-cache", action="store_true", help="сбросить кэш fair value")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    if args.reset_cache:
        reset_cache()
        return

    if args.once:
        recompute_all()
    else:
        daemon_loop()


if __name__ == "__main__":
    main()