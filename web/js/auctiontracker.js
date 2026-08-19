const RARITY = {
  0: { label: "Обычный",        color: "#8a95a3", glow: "rgba(138,149,163,0.2)",   bg: "rgba(138,149,163,0.06)" },
  1: { label: "Необычный",      color: "#5fd48a", glow: "rgba(95,212,138,0.25)",   bg: "rgba(95,212,138,0.07)" },
  2: { label: "Особый",         color: "#4fa3f7", glow: "rgba(79,163,247,0.25)",   bg: "rgba(79,163,247,0.07)" },
  3: { label: "Редкий",         color: "#c97cf0", glow: "rgba(201,124,240,0.25)",  bg: "rgba(201,124,240,0.07)" },
  4: { label: "Исключительный", color: "#ff8a5c", glow: "rgba(255,138,92,0.25)",   bg: "rgba(255,138,92,0.07)" },
  5: { label: "Легендарный",    color: "#ffcf4d", glow: "rgba(255,207,77,0.3)",    bg: "rgba(255,207,77,0.08)" },
};

// Диапазоны скрытого процента по качеству (qlt)
const QLT_RANGES = {
  0: [0, 100],
  1: [100, 115],
  2: [115, 130],
  3: [130, 145],
  4: [145, 160],
  5: [160, 175],
};

// Процент артефакта для изученных предметов (есть stats_random)
function computeArtifactPercent(qlt, statsRandom) {
  const [minPct, maxPct] = QLT_RANGES[qlt] || QLT_RANGES[0];
  const fraction = Math.max(0, Math.min(1, (statsRandom + 2) / 4));
  return Math.round((minPct + fraction * (maxPct - minPct)) * 100) / 100;
}

const STAT_LABELS = {
  STAMINA_REGENERATION: "Восстановление выносливости",
  HEALTH_BONUS: "Живучесть",
  LIVELINESS: "Живучесть",
  REGENERATION_BONUS: "Регенерация здоровья",
  MAX_WEIGHT_BONUS: "Переносимый вес",
  STAMINA_BONUS: "Выносливость",
  BULLET_DMG: "Пулестойкость",
  WIGGLE_BONUS: "Покачивание",
  RECOIL_BONUS: "Отдача",
  HEAL_EFFICIENCY: "Эффективность лечения",
  EXPLOSION_DMG: "Защита от взрыва",
  SPEED_MOD: "Скорость передвижения",
  RADIATION_PROTECTION: "Защита от радиации",
  ELECTRA_DMG: "Электрозащита",
  BLEEDING_ACC: "Кровотечение",
  BLEEDING_PROTECTION: "Защита от кровотечения",
  PSYCHO_ACC: "Пси-излучение",
  COMBUSTION_ACC: "Горение",
  BIOLOGICAL_ACC: "Биологическое заражение",
  STOPPING_PROTECTION: "Стойкость",
  TEAR_DMG: "Защита от разрыва",
  THERMAL_ACC: "Температура",
  BIOLOGICAL_PROTECTION: "Защита от биозаражения",
  THERMAL_PROTECTION: "Защита от температуры",
  PSYCHO_PROTECTION: "Защита от пси-излучения",
  RADIATION_ACC: "Радиация",
  ARTEFAKT_HEAL: "Периодическое лечение",
};

const ICONS = {
  chevron: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m9 18 6-6-6-6"/></svg>`,
  search: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>`,
  clock: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`,
};

let ITEMS = {};
let RAW_LOTS = [];
let TOTAL_LOTS = 0;
let HISTORY = {};
let FETCHED_AT = null;
let LAST_ACTIVE_RUN = null;
let BRAIN_READY = false;
let HISTORY_LOADING = false;
let COUNTDOWN_INTERVAL_ID = null;
let historyRequestSeq = 0;
const HISTORY_MAX_HOURS = 2160; // 90 дней — максимальный период, кэшируем всё сразу

async function fetchJson(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`failed to load ${url}`);
  return resp.json();
}

async function loadRealData() {
  const payload = await fetchJson("/api/auction-data");
  ITEMS = payload.items || {};
  const flat = Array.isArray(payload.lots) ? payload.lots : [];
  TOTAL_LOTS = payload.total_lots || 0;
  FETCHED_AT = payload.fetched_at || null;
  LAST_ACTIVE_RUN = payload.last_active_run || null;
  BRAIN_READY = !!payload.brain_ready;
  HISTORY = {};
  return flat;
}

function autoSelectRange() {
  // Выбираем наименьший период, в котором есть хотя бы одна продажа
  // с учётом текущих фильтров (качество + заточка).
  const records = HISTORY[modalState.itemId] || [];
  for (const hours of RANGE_OPTIONS) {
    if (filterHistoryRecords(records, hours).length > 0) {
      modalState.rangeHours = hours;
      updateRangeButtons();
      return;
    }
  }
  // Если продаж нет ни за один период — оставляем 90д, чтобы показать пустоту
  modalState.rangeHours = RANGE_OPTIONS[RANGE_OPTIONS.length - 1];
  updateRangeButtons();
}

async function loadHistoryForItem(itemId) {
  if (!itemId) return;
  // Если данные за 90д уже загружены — не перезагружаем, просто рендерим
  if (HISTORY[itemId] && HISTORY[itemId].length > 0) {
    if (modalState.exactMatch) autoSelectRange();
    renderHistoryModal();
    return;
  }
  const seq = ++historyRequestSeq;
  HISTORY_LOADING = true;
  try {
    const payload = await fetchJson(`/api/history/${encodeURIComponent(itemId)}?hours=${HISTORY_MAX_HOURS}`);
    const records = Array.isArray(payload) ? payload : (payload.history || []);
    // Игнорируем устаревший ответ, если пользователь уже выбрал другой айтем
    if (seq !== historyRequestSeq) return;
    HISTORY[itemId] = Array.isArray(records) ? records : [];
  } catch (e) {
    if (seq !== historyRequestSeq) return;
    HISTORY[itemId] = [];
  } finally {
    if (seq === historyRequestSeq) {
      HISTORY_LOADING = false;
      if (modalState.exactMatch) autoSelectRange();
      renderHistoryModal();
    }
  }
}

function generateMockLots(count = 24) {
  const names = ["Гребешок", "Солнце", "Кисель", "Остов", "Слизень", "Игла", "Ветка", "Уголёк"];
  const statKeys = Object.keys(STAT_LABELS);
  ITEMS = {};
  const lots = [];
  for (let i = 0; i < count; i++) {
    const itemId = "mock_" + i;
    const qlt = Math.floor(Math.random() * 6);
    const hasPtn = Math.random() > 0.35;
    ITEMS[itemId] = { name: names[i % names.length], name_en: "", icon: null };
    const bonusCount = Math.floor(Math.random() * 3);
    const bonus_properties = [];
    for (let b = 0; b < bonusCount; b++) {
      const k = statKeys[Math.floor(Math.random() * statKeys.length)];
      if (!bonus_properties.includes(k)) bonus_properties.push(k);
    }
    const price = Math.floor(Math.random() * 800000 / 1000) * 1000 + 15000;
    const fv = price * (1 + (Math.random() - 0.3) * 0.6);
    lots.push({
      itemId,
      amount: 1,
      startPrice: Math.floor(price * 0.9),
      buyoutPrice: price,
      startTime: new Date().toISOString(),
      endTime: new Date(Date.now() + Math.random() * 1000 * 60 * 60 * 40).toISOString(),
      additional: { bonus_properties, qlt, ptn: hasPtn ? Math.floor(Math.random() * 15) + 1 : null },
      score: Math.random() > 0.25 ? {
        fairValue: Math.round(fv),
        absoluteProfit: Math.round(fv - price),
        percentProfit: ((fv - price) / price) * 100,
        salesPerDay: Math.random() * 6,
        expectedDaysToSell: Math.random() * 14,
        lowConfidence: Math.random() > 0.7,
      } : null,
    });
  }
  FETCHED_AT = "демо-данные";
  return lots;
}

function fmtRub(n) { return Math.round(n).toLocaleString("ru-RU") + " ₽"; }

function fmtSellDays(days) {
  if (days == null || !isFinite(days)) return "—";
  if (days < 1) return `~${Math.round(days * 24)} ч`;
  return `~${Math.round(days)} дн`;
}

function fmtRemaining(endTimeIso) {
  const ms = new Date(endTimeIso).getTime() - Date.now();
  if (ms <= 0) return { text: "истёк", soon: true };
  const totalSeconds = Math.floor(ms / 1000);
  const h = Math.floor(totalSeconds / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = totalSeconds % 60;
  const soon = ms < 1000 * 60 * 60 * 2;
  const text = h > 0 ? `${h}ч ${m}м ${s}с` : (m > 0 ? `${m}м ${s}с` : `${s}с`);
  return { text, soon };
}

function updateCountdowns() {
  document.querySelectorAll(".expires[data-end-time]").forEach(el => {
    const remaining = fmtRemaining(el.dataset.endTime);
    el.innerHTML = `${remaining.soon ? ICONS.clock : ''}<span>${remaining.text}</span>`;
    el.classList.toggle("soon", remaining.soon);
  });
}

function renderCard(lot, index) {
  const additional = lot.additional || {};
  const qlt = additional.qlt ?? 0;
  const rarity = RARITY[qlt] || RARITY[0];
  const itemMeta = ITEMS[lot.itemId] || { name: lot.itemId, icon: null };
  const ptnLabel = additional.ptn ? `+${additional.ptn}` : "";
  const remaining = fmtRemaining(lot.endTime);

  const tags = (additional.bonus_properties || [])
    .map(key => `<span class="tag">${STAT_LABELS[key] || key}</span>`)
    .join("");

  const iconHtml = itemMeta.icon
    ? `<img src="${itemMeta.icon}" alt="">`
    : `<span class="diamond">◆</span>`;

  const score = lot.score;
  let profitBlock;
  if (score && score.fairValue != null) {
    const isPositive = score.absoluteProfit >= 0;
    const profitColor = isPositive ? "#6fe0a0" : "#ff6b5c";
    const sales24h = score.salesPerDay != null ? Math.round(score.salesPerDay) : 0;
    const lowSales = score.salesPerDay != null ? score.salesPerDay < 1 : true;
    const showWarning = score.lowConfidence || lowSales;
    const sellDays = fmtSellDays(score.expectedDaysToSell);
    const coolingBadge = score.marketCooling
      ? `<div class="low-conf cooling">${ICONS.clock} рынок остывает</div>`
      : '';
    const nextTierHint = score.nextTierPtn != null && score.nextTierPrice != null
      ? `<div class="next-tier-hint">💡 +${score.nextTierPtn} в среднем: ${fmtRub(score.nextTierPrice)}</div>`
      : '';
    const targetPrice = score.targetPrice != null ? fmtRub(score.targetPrice) : fmtRub(score.fairValue);
    const competitors = score.competitorsBelow != null ? score.competitorsBelow : 0;
    profitBlock = `
      <div class="divider" style="margin:5px 0 9px;"></div>
      <div class="price-row"><span class="k">Целевая цена</span><span class="fv">${fmtRub(score.fairValue)}</span></div>
      <div class="price-row">
        <span class="k">Профит</span>
        <span class="profit${isPositive ? ' profit-positive' : ''}" style="color:${profitColor}">
          ${isPositive ? "+" : ""}${fmtRub(score.absoluteProfit)}<span class="pct">(${score.percentProfit.toFixed(1)}%)</span>
        </span>
      </div>
      <div class="price-row"><span class="k">Продастся за</span><span class="fv">${sellDays}</span></div>
      <div class="price-row"><span class="k">Цена продажи</span><span class="fv">${targetPrice}</span></div>
      ${competitors > 0 ? `<div class="price-row"><span class="k">Конкурентов ниже</span><span class="fv">${competitors}</span></div>` : ''}
      ${showWarning ? `<div class="low-conf">${ICONS.clock} Продаж за 24ч — ${sales24h} ⚠</div>` : ''}
      ${coolingBadge}
      ${nextTierHint}
    `;
  } else {
    profitBlock = `<div class="not-computed">расчёт ещё не выполнен…</div>`;
  }

  const isGone = lot.status === "gone";
  const goneClass = isGone ? " gone" : "";
  const goneBadge = isGone ? `<div class="gone-badge">продан</div>` : "";

  const statsRandom = additional.stats_random;
  const percentLabel = statsRandom != null
    ? `<span class="lot-percent">${computeArtifactPercent(qlt, statsRandom)}%</span>`
    : `<span class="lot-percent unstudied">не изучен</span>`;

  const appearedLabel = lot.firstSeenAt
    ? `<span class="appeared-badge" title="Появился на аукционе: ${new Date(lot.firstSeenAt).toLocaleString('ru-RU')}">${ICONS.clock} ${fmtAgo(lot.firstSeenAt)}</span>`
    : '';

  const delay = Math.min(index * 30, 400);

  return `
    <div class="lot-card${goneClass}" style="--rarity-color:${rarity.color};--rarity-glow:${rarity.glow};--rarity-bg:${rarity.bg};animation-delay:${delay}ms" data-item-id="${lot.itemId}" data-qlt="${additional.qlt ?? 0}" data-ptn="${additional.ptn ?? ''}">
      ${goneBadge}
      <div class="lot-body">
        <div class="lot-head">
          <div class="lot-icon">${iconHtml}</div>
          <div class="lot-name-wrap">
            <div class="lot-name" title="${itemMeta.name}${additional.ptn ? ` +${additional.ptn}` : ""}">
              ${itemMeta.name}${ptnLabel ? `<span class="ptn">${ptnLabel}</span>` : ""}${percentLabel}
            </div>
            <div class="lot-sub">
              <span class="rarity-badge">${rarity.label}</span>
              ${appearedLabel}
            </div>
          </div>
        </div>
        <div class="lot-tags">
          ${tags || '<span class="no-tags">без бонусных свойств</span>'}
        </div>
        <div class="divider"></div>
        <div class="price-block">
          <div class="price-row"><span class="k">Цена выкупа</span><span class="price">${fmtRub(lot.buyoutPrice)}</span></div>
          ${profitBlock}
        </div>
        <div class="lot-footer">
          <span class="expires ${remaining.soon ? "soon" : ""}" data-end-time="${lot.endTime}">
            ${remaining.soon ? ICONS.clock : ''}<span>${remaining.text}</span>
          </span>
          <button class="lot-chevron" title="История цен для этого предмета">${ICONS.chevron}</button>
        </div>
      </div>
    </div>
  `;
}

const PAGE_SIZE = 100;
const state = {
  name: "", rarity: "all", ptn: 0,
  minProfit: null, minPercent: null,
  minSales: null, minFv: null,
  minPrice: null, maxPrice: null, maxDays: null, hideLow: false,
  sortMode: "score", visibleCount: PAGE_SIZE,
};

function sortLots(lots) {
  const mode = state.sortMode;
  const sorted = [...lots];
  if (mode === "profit") {
    sorted.sort((a, b) => ((b.score && b.score.absoluteProfit) ?? -Infinity) - ((a.score && a.score.absoluteProfit) ?? -Infinity));
  } else if (mode === "newest") {
    sorted.sort((a, b) => (new Date(b.firstSeenAt || 0).getTime()) - (new Date(a.firstSeenAt || 0).getTime()));
  } else {
    sorted.sort((a, b) => ((b.score && b.score.score) ?? 0) - ((a.score && a.score.score) ?? 0));
  }
  return sorted;
}

function applyFilters() {
  const filtered = sortLots(RAW_LOTS.filter(lot => {
    const itemMeta = ITEMS[lot.itemId] || { name: "" };
    const additional = lot.additional || {};
    if (state.name && !itemMeta.name.toLowerCase().includes(state.name.toLowerCase())) return false;
    if (state.rarity !== "all" && (additional.qlt ?? 0) !== Number(state.rarity)) return false;
    if (state.ptn === 1) {
      if (additional.ptn != null && additional.ptn !== 0) return false;
    } else if (state.ptn > 1) {
      if (!additional.ptn || additional.ptn < state.ptn - 1) return false;
    }
    if (state.minPrice != null && lot.buyoutPrice < state.minPrice) return false;
    if (state.maxPrice != null && lot.buyoutPrice > state.maxPrice) return false;
    if (lot.score) {
      if (state.minProfit != null && lot.score.absoluteProfit < state.minProfit) return false;
      if (state.minPercent != null && lot.score.percentProfit < state.minPercent) return false;
      if (state.minSales != null && (lot.score.salesPerDay ?? 0) < state.minSales) return false;
      if (state.minFv != null && lot.score.fairValue < state.minFv) return false;
      if (state.maxDays != null && lot.score.expectedDaysToSell != null && lot.score.expectedDaysToSell > state.maxDays) return false;
      if (state.hideLow && lot.score.lowConfidence) return false;
    }
    return true;
  }));

  const grid = document.getElementById("lots-grid");
  document.getElementById("result-count").textContent = filtered.length;
  document.getElementById("total-count").textContent = RAW_LOTS.length;
  const grandEl = document.getElementById("grand-total");
  if (grandEl) grandEl.textContent = TOTAL_LOTS || RAW_LOTS.length;

  const loadMoreWrap = document.getElementById("load-more-wrap");
  if (filtered.length === 0) {
    grid.innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">${ICONS.search}</div>
        <p>Ничего не найдено — попробуй ослабить фильтры</p>
      </div>
    `;
    loadMoreWrap.style.display = "none";
    return;
  }

  const visible = filtered.slice(0, state.visibleCount);
  grid.innerHTML = visible.map((lot, i) => renderCard(lot, i)).join("");
  loadMoreWrap.style.display = filtered.length > state.visibleCount ? "block" : "none";
  updateCountdowns();
}

// Клик по названию лота — скопировать только название в буфер обмена.
// Клик по стрелке — открыть историю именно этого предмета с тем же
// качеством (qlt) и заточкой (ptn).
document.getElementById("lots-grid").addEventListener("click", (e) => {
  const nameEl = e.target.closest(".lot-name");
  if (nameEl) {
    const card = nameEl.closest(".lot-card");
    if (!card) return;
    e.stopPropagation();
    const itemId = card.dataset.itemId;
    const meta = ITEMS[itemId];
    const copyName = meta && meta.name ? meta.name : itemId;
    const toast = document.getElementById("copy-toast");
    if (toast) {
      toast.textContent = `Скопировано: ${copyName}`;
      toast.classList.add("show");
      clearTimeout(toast._t);
      toast._t = setTimeout(() => toast.classList.remove("show"), 1800);
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(copyName).catch(() => {});
    } else {
      const ta = document.createElement("textarea");
      ta.value = copyName;
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand("copy"); } catch (_) {}
      document.body.removeChild(ta);
    }
    return;
  }

  const chevron = e.target.closest(".lot-chevron");
  if (!chevron) return;
  const card = chevron.closest(".lot-card");
  if (!card) return;
  e.stopPropagation();
  const itemId = card.dataset.itemId;
  const qlt = card.dataset.qlt;
  const ptn = card.dataset.ptn;
  if (!itemId) return;
  openLotHistory(itemId, qlt, ptn);
});

const ptnSlider = document.getElementById("f-ptn");
const ptnVal = document.getElementById("f-ptn-val");
function setupRangeBackground(slider, valueEl) {
  function paint() {
    const v = Number(slider.value);
    const max = Number(slider.max);
    const pct = max > 0 ? (v / max) * 100 : 0;
    slider.style.background = `linear-gradient(to right, #9dff5c 0%, #9dff5c ${pct}%, #1a2230 ${pct}%, #1a2230 100%)`;
    if (valueEl) {
      if (v === 0) valueEl.textContent = "Все";
      else if (v === 1) valueEl.textContent = "0";
      else valueEl.textContent = "+" + (v - 1);
    }
  }
  slider.addEventListener("input", paint);
  paint();
}
setupRangeBackground(ptnSlider, ptnVal);

document.getElementById("f-name").addEventListener("input", e => { state.name = e.target.value; applyFilters(); });
document.getElementById("f-rarity").addEventListener("change", e => { state.rarity = e.target.value; applyFilters(); });
ptnSlider.addEventListener("input", e => { state.ptn = Number(e.target.value); applyFilters(); });
document.getElementById("f-min-profit").addEventListener("input", e => { state.minProfit = e.target.value === "" ? null : Number(e.target.value); applyFilters(); });
document.getElementById("f-min-percent").addEventListener("input", e => { state.minPercent = e.target.value === "" ? null : Number(e.target.value); applyFilters(); });
document.getElementById("f-min-sales").addEventListener("input", e => { state.minSales = e.target.value === "" ? null : Number(e.target.value); applyFilters(); });
document.getElementById("f-min-fv").addEventListener("input", e => { state.minFv = e.target.value === "" ? null : Number(e.target.value); applyFilters(); });
document.getElementById("f-min-price").addEventListener("input", e => { state.minPrice = e.target.value === "" ? null : Number(e.target.value); applyFilters(); });
document.getElementById("f-max-price").addEventListener("input", e => { state.maxPrice = e.target.value === "" ? null : Number(e.target.value); applyFilters(); });
document.getElementById("f-max-days").addEventListener("input", e => { state.maxDays = e.target.value === "" ? null : Number(e.target.value); applyFilters(); });
document.getElementById("f-hide-low").addEventListener("change", e => { state.hideLow = e.target.checked; state.visibleCount = PAGE_SIZE; applyFilters(); });
document.getElementById("f-sort").addEventListener("change", e => { state.sortMode = e.target.value; state.visibleCount = PAGE_SIZE; applyFilters(); });
document.getElementById("btn-load-more").addEventListener("click", () => {
  state.visibleCount += PAGE_SIZE;
  applyFilters();
});

document.getElementById("btn-reset").addEventListener("click", () => {
  state.name = ""; state.rarity = "all"; state.ptn = 0;
  state.minProfit = null; state.minPercent = null;
  state.minSales = null; state.minFv = null; state.minPrice = null; state.maxPrice = null; state.maxDays = null; state.hideLow = false;
  document.getElementById("f-name").value = "";
  document.getElementById("f-rarity").value = "all";
  document.getElementById("f-min-profit").value = "";
  document.getElementById("f-min-percent").value = "";
  document.getElementById("f-min-sales").value = "";
  document.getElementById("f-min-fv").value = "";
  document.getElementById("f-min-price").value = "";
  document.getElementById("f-max-price").value = "";
  document.getElementById("f-max-days").value = "";
  document.getElementById("f-hide-low").checked = false;
  document.getElementById("f-sort").value = "score";
  state.sortMode = "score";
  state.visibleCount = PAGE_SIZE;
  ptnSlider.value = 0;
  setupRangeBackground(ptnSlider, ptnVal);
  applyFilters();
});
document.getElementById("btn-refresh").addEventListener("click", () => init());

document.getElementById("btn-clear-gone").addEventListener("click", async () => {
  const btn = document.getElementById("btn-clear-gone");
  btn.disabled = true;
  try {
    await fetch("/api/clear-gone", { method: "POST" });
    await refreshLiveData();
  } catch (e) {
    console.warn("Clear gone failed:", e);
  } finally {
    btn.disabled = false;
  }
});

async function refreshLiveData() {
  try {
    const payload = await fetchJson("/api/auction-data");
    ITEMS = payload.items || ITEMS;
    const flat = Array.isArray(payload.lots) ? payload.lots : [];
    if (flat.length > 0) RAW_LOTS = flat;
    FETCHED_AT = payload.fetched_at || FETCHED_AT;
    LAST_ACTIVE_RUN = payload.last_active_run || LAST_ACTIVE_RUN;
    BRAIN_READY = !!payload.brain_ready;
    document.getElementById("fallback-banner").classList.remove("show");
    const brainNote = BRAIN_READY ? "отфильтровано мозгом" : "мозг ещё не запускался";
    document.getElementById("data-status").textContent = `реальные данные · ${RAW_LOTS.length} лотов · ${brainNote}`;
    updateFetchedStatus();
    populateHistoryItemSelect();
    applyFilters();
    // Если модалка истории открыта — перерисовать, чтобы название артефакта не пропадало
    if (document.getElementById("modal-overlay").classList.contains("show") && modalState.itemId) {
      renderHistoryModal();
    }
  } catch (e) {
    console.warn("Live refresh failed:", e);
  }
}

function fmtAgo(ts) {
  const t = new Date(ts).getTime();
  if (!t) return "";
  const sec = Math.max(0, Math.floor((Date.now() - t) / 1000));
  if (sec < 60) return "только что";
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} мин назад`;
  const h = Math.floor(min / 60);
  if (h < 24) return `${h} ч назад`;
  return `${Math.floor(h / 24)} дн назад`;
}

function updateFetchedStatus() {
  const el = document.getElementById("fetched-at");
  if (LAST_ACTIVE_RUN) {
    const time = new Date(LAST_ACTIVE_RUN).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    el.textContent = `${time} · ${fmtAgo(LAST_ACTIVE_RUN)}`;
  } else if (FETCHED_AT) {
    const time = new Date(FETCHED_AT).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    el.textContent = time;
  } else {
    el.textContent = "—";
  }
}

function updateScanProgress(data) {
  const bar = document.getElementById("scan-progress-bar");
  const fetchedEl = document.getElementById("scan-progress-fetched");
  const computedEl = document.getElementById("scan-progress-computed");
  if (!bar || !fetchedEl || !computedEl || !data) return;

  const total = data.total || 0;
  const fetchedPct = total ? Math.min(100, (data.fetched / total) * 100) : 0;
  const computedPct = total ? Math.min(100, (data.computed / total) * 100) : 0;
  fetchedEl.style.width = fetchedPct + "%";
  computedEl.style.width = computedPct + "%";

  if (data.active) {
    bar.classList.add("active");
  } else {
    // недолгая пауза, чтобы глаз успел увидеть 100%, потом гасим и сбрасываем
    setTimeout(() => {
      bar.classList.remove("active");
      fetchedEl.style.width = "0%";
      computedEl.style.width = "0%";
    }, 900);
  }
}

function startLiveUpdates() {
  if (COUNTDOWN_INTERVAL_ID) clearInterval(COUNTDOWN_INTERVAL_ID);
  COUNTDOWN_INTERVAL_ID = setInterval(() => {
    updateCountdowns();
    updateFetchedStatus();
  }, 1000);

  if (typeof EventSource !== "undefined") {
    const es = new EventSource("/api/events");
    es.addEventListener("data-updated", () => { void refreshLiveData(); });
    es.addEventListener("scan-progress", (e) => {
      try { updateScanProgress(JSON.parse(e.data)); } catch (err) { /* ignore */ }
    });
    es.onerror = () => {};
  }

  // Подхватить текущий прогресс, если вкладку открыли посреди уже идущего скана
  fetchJson("/api/scan-progress").then(updateScanProgress).catch(() => {});
}

async function init() {
  const refreshBtn = document.getElementById("btn-refresh");
  const refreshIcon = document.getElementById("refresh-icon");
  document.getElementById("data-status").textContent = "загрузка данных…";
  refreshBtn.disabled = true;
  refreshIcon.classList.add("spinning");
  try {
    RAW_LOTS = await loadRealData();
    document.getElementById("fallback-banner").classList.remove("show");
    const brainNote = BRAIN_READY ? "отфильтровано мозгом" : "мозг ещё не запускался";
    document.getElementById("data-status").textContent = `реальные данные · ${RAW_LOTS.length} лотов · ${brainNote}`;
  } catch (e) {
    RAW_LOTS = generateMockLots();
    document.getElementById("fallback-banner").classList.add("show");
    document.getElementById("data-status").textContent = "демо-данные (сервер недоступен)";
  }
  updateFetchedStatus();
  applyFilters();
  populateHistoryItemSelect();
  startLiveUpdates();
  refreshBtn.disabled = false;
  refreshIcon.classList.remove("spinning");
}

/* ====== HISTORY MODAL ====== */
const modalState = {
  itemId: "", rarity: "all", ptn: 0, rangeHours: 24,
  exactMatch: false, exactQlt: null, exactPtn: null,
  ptnExact: true,
};
let chartInstance = null;
const SALES_PAGE_SIZE = 500;
let salesVisibleCount = SALES_PAGE_SIZE;
const RANGE_OPTIONS = [24, 72, 168, 720, 2160];

function populateHistoryItemSelect() {
  const select = document.getElementById("m-item");
  if (!select) return;
  const entries = Object.entries(ITEMS).sort((a, b) => a[1].name.localeCompare(b[1].name, "ru"));
  select.innerHTML = '<option value="">— Выберите артефакт —</option>' +
    entries.map(([id, meta]) => `<option value="${id}">${meta.name}</option>`).join("");
  // Восстанавливаем выбранный артефакт, чтобы название не пропадало при live-обновлении
  if (modalState.itemId && ITEMS[modalState.itemId]) {
    select.value = modalState.itemId;
  }
}

function syncPtnExactCheckbox() {
  const cb = document.getElementById("m-ptn-exact");
  if (cb) cb.checked = modalState.ptnExact;
}

function updatePtnExactVisibility() {
  const wrap = document.getElementById("m-ptn-exact-wrap");
  if (wrap) wrap.style.display = modalState.ptn === 0 ? "none" : "flex";
}

function updateRangeButtons() {
  document.querySelectorAll("#m-range button").forEach(b => {
    b.classList.toggle("active", Number(b.dataset.hours) === modalState.rangeHours);
  });
}

function openHistoryModal() {
  modalState.exactMatch = false;
  modalState.exactQlt = null;
  modalState.exactPtn = null;
  modalState.ptnExact = true;
  syncPtnExactCheckbox();
  document.getElementById("modal-overlay").classList.add("show");
  renderHistoryModal();
}

function openLotHistory(itemId, qlt, ptn) {
  const ptnValue = (ptn == null || ptn === "") ? null : Number(ptn);
  modalState.itemId = itemId;
  modalState.rarity = String(qlt);
  modalState.ptn = ptnValue != null ? ptnValue + 1 : 1;
  modalState.rangeHours = 24;
  modalState.exactMatch = true;
  modalState.exactQlt = Number(qlt);
  modalState.exactPtn = ptnValue;
  modalState.ptnExact = true;

  const itemSelect = document.getElementById("m-item");
  if (itemSelect) itemSelect.value = itemId;

  const raritySelect = document.getElementById("m-rarity");
  if (raritySelect) raritySelect.value = String(qlt);

  const ptnSliderEl = document.getElementById("m-ptn");
  if (ptnSliderEl) {
    ptnSliderEl.value = modalState.ptn;
    setupRangeBackground(ptnSliderEl, mPtnVal);
  }
  syncPtnExactCheckbox();

  document.getElementById("modal-overlay").classList.add("show");
  if (itemId) void loadHistoryForItem(itemId);
  else renderHistoryModal();
}

function closeHistoryModal() {
  document.getElementById("modal-overlay").classList.remove("show");
}

document.getElementById("btn-open-history").addEventListener("click", openHistoryModal);
document.getElementById("modal-close").addEventListener("click", closeHistoryModal);
document.getElementById("modal-overlay").addEventListener("click", (e) => {
  if (e.target.id === "modal-overlay") closeHistoryModal();
});
document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeHistoryModal(); });

document.getElementById("m-item").addEventListener("change", (e) => {
  modalState.exactMatch = false;
  modalState.exactQlt = null;
  modalState.exactPtn = null;
  modalState.ptnExact = true;
  syncPtnExactCheckbox();
  modalState.itemId = e.target.value;
  resetSalesPagination();
  if (modalState.itemId) void loadHistoryForItem(modalState.itemId);
  else renderHistoryModal();
});
document.getElementById("m-rarity").addEventListener("change", (e) => {
  modalState.exactMatch = false;
  modalState.exactQlt = null;
  modalState.exactPtn = null;
  modalState.ptnExact = true;
  syncPtnExactCheckbox();
  modalState.rarity = e.target.value;
  resetSalesPagination();
  renderHistoryModal();
});

const mPtnSlider = document.getElementById("m-ptn");
const mPtnVal = document.getElementById("m-ptn-val");
setupRangeBackground(mPtnSlider, mPtnVal);
mPtnSlider.addEventListener("input", (e) => {
  modalState.exactMatch = false;
  modalState.exactQlt = null;
  modalState.exactPtn = null;
  modalState.ptn = Number(e.target.value);
  updatePtnExactVisibility();
  resetSalesPagination();
  renderHistoryModal();
});

document.getElementById("m-ptn-exact").addEventListener("change", (e) => {
  modalState.ptnExact = e.target.checked;
  resetSalesPagination();
  renderHistoryModal();
});

document.querySelectorAll("#m-range button").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll("#m-range button").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    modalState.rangeHours = Number(btn.dataset.hours);
    // Данные за 90д уже в кэше — просто перерисовываем
    resetSalesPagination();
    renderHistoryModal();
  });
});

function resetSalesPagination() {
  salesVisibleCount = SALES_PAGE_SIZE;
  const moreWrap = document.getElementById("m-sales-more-wrap");
  if (moreWrap) moreWrap.style.display = "none";
}

function updateSalesMoreButton() {
  const list = document.getElementById("m-sales-list");
  const moreWrap = document.getElementById("m-sales-more-wrap");
  if (!list || !moreWrap) return;
  const records = getFilteredHistory();
  const sorted = [...records].sort((a, b) => new Date(b.time) - new Date(a.time));
  const atBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 10;
  const hasMore = sorted.length > salesVisibleCount;
  moreWrap.style.display = (atBottom && hasMore) ? "block" : "none";
}

document.getElementById("m-sales-list").addEventListener("scroll", updateSalesMoreButton);

document.getElementById("m-sales-more").addEventListener("click", () => {
  salesVisibleCount += SALES_PAGE_SIZE;
  renderHistoryModal();
  const list = document.getElementById("m-sales-list");
  if (list) list.scrollTop = list.scrollHeight;
});

function filterHistoryRecords(records, rangeHours) {
  const cutoff = Date.now() - rangeHours * 3600 * 1000;
  return records.filter(r => {
    const t = new Date(r.time).getTime();
    if (t < cutoff) return false;

    // Режим точного совпадения — открыт со стрелки на карточке лота:
    // показываем историю только того же качества (qlt) и заточки (ptn).
    if (modalState.exactMatch) {
      if ((r.qlt ?? 0) !== modalState.exactQlt) return false;
      const recPtn = (r.ptn == null) ? null : Number(r.ptn);
      if (recPtn !== modalState.exactPtn) return false;
      return true;
    }

    if (modalState.rarity !== "all" && (r.qlt ?? 0) !== Number(modalState.rarity)) return false;

    // Чекбокс «Только этот уровень» включён — показываем только ровно этот уровень.
    // Выключен — показываем все начиная с указанного уровня.
    if (modalState.ptnExact) {
      if (modalState.ptn === 1) {
        if ((r.ptn ?? 0) !== 0) return false;
      } else if (modalState.ptn > 1) {
        if ((r.ptn ?? 0) !== modalState.ptn - 1) return false;
      }
    } else {
      if (modalState.ptn === 1) {
        if (r.ptn != null && r.ptn !== 0) return false;
      } else if (modalState.ptn > 1) {
        if (!r.ptn || r.ptn < modalState.ptn - 1) return false;
      }
    }
    return true;
  });
}

function getFilteredHistory() {
  if (!modalState.itemId) return [];
  const records = HISTORY[modalState.itemId] || [];
  return filterHistoryRecords(records, modalState.rangeHours);
}

function bucketSizeMs(rangeHours) {
  if (rangeHours <= 24) return 3600 * 1000;
  if (rangeHours <= 72) return 3 * 3600 * 1000;
  if (rangeHours <= 168) return 6 * 3600 * 1000;
  if (rangeHours <= 720) return 24 * 3600 * 1000;
  return 3 * 24 * 3600 * 1000;
}

function buildChartSeries(records, rangeHours) {
  const bucketMs = bucketSizeMs(rangeHours);
  const now = Date.now();
  const start = now - rangeHours * 3600 * 1000;
  const bucketCount = Math.ceil((now - start) / bucketMs);
  const sums = new Array(bucketCount).fill(0);
  const counts = new Array(bucketCount).fill(0);

  for (const r of records) {
    const t = new Date(r.time).getTime();
    let idx = Math.floor((t - start) / bucketMs);
    idx = Math.max(0, Math.min(bucketCount - 1, idx));
    sums[idx] += r.price;
    counts[idx] += 1;
  }

  const labels = [];
  const values = [];
  for (let i = 0; i < bucketCount; i++) {
    const bucketStart = new Date(start + i * bucketMs);
    const label = rangeHours <= 168
      ? bucketStart.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })
      : bucketStart.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" });
    labels.push(label);
    values.push(counts[i] > 0 ? Math.round(sums[i] / counts[i]) : null);
  }
  return { labels, values };
}

function renderChart(records) {
  const canvas = document.getElementById("m-chart");
  const empty = document.getElementById("m-chart-empty");

  if (!modalState.itemId || records.length === 0) {
    canvas.style.display = "none";
    empty.style.display = "flex";
    empty.innerHTML = !modalState.itemId
      ? `<span class="chart-empty-icon">${ICONS.search}</span><span>Выберите артефакт</span>`
      : `<span class="chart-empty-icon">${ICONS.search}</span><span>Нет продаж за выбранный период</span>`;
    if (chartInstance) { chartInstance.destroy(); chartInstance = null; }
    return;
  }

  canvas.style.display = "block";
  empty.style.display = "none";

  const { labels, values } = buildChartSeries(records, modalState.rangeHours);

  if (typeof Chart === "undefined") {
    canvas.style.display = "none";
    empty.style.display = "flex";
    empty.innerHTML = `<span class="chart-empty-icon">${ICONS.search}</span><span>Chart.js не загрузился</span>`;
    return;
  }

  if (chartInstance) chartInstance.destroy();
  try {
    chartInstance = new Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels,
        datasets: [{
          data: values,
          borderColor: "#9dff5c",
          backgroundColor: (ctx) => {
            const gradient = ctx.chart.ctx.createLinearGradient(0, 0, 0, ctx.chart.height);
            gradient.addColorStop(0, "rgba(157,255,92,0.18)");
            gradient.addColorStop(1, "rgba(157,255,92,0.01)");
            return gradient;
          },
          fill: true,
          tension: 0.4,
          spanGaps: true,
          pointRadius: 0,
          pointHoverRadius: 5,
          pointHoverBackgroundColor: "#9dff5c",
          pointHoverBorderColor: "#060910",
          pointHoverBorderWidth: 2,
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "#0c1016",
            borderColor: "#1a2230",
            borderWidth: 1,
            titleColor: "#8b96a3",
            bodyColor: "#dce4ec",
            titleFont: { family: "Geist Mono", size: 10 },
            bodyFont: { family: "Geist Mono", size: 12, weight: "600" },
            callbacks: { label: (ctx) => ctx.raw != null ? ctx.raw.toLocaleString("ru-RU") + " ₽" : "—" },
          },
        },
        scales: {
          x: {
            ticks: { color: "#475566", maxTicksLimit: 8, font: { family: "Geist Mono", size: 10 } },
            grid: { color: "#1a2230" },
            border: { color: "#1a2230" },
          },
          y: {
            ticks: {
              color: "#475566",
              font: { family: "Geist Mono", size: 10 },
              callback: (v) => (v / 1000).toFixed(0) + "к",
            },
            grid: { color: "#1a2230" },
            border: { color: "#1a2230" },
          },
        },
      },
    });
  } catch (e) {
    console.error("Chart render failed:", e);
    canvas.style.display = "none";
    empty.style.display = "flex";
    empty.innerHTML = `<span class="chart-empty-icon">${ICONS.search}</span><span>Ошибка построения графика</span>`;
  }
}

function renderStats(records) {
  const avgEl = document.getElementById("m-avg-price");
  const changeEl = document.getElementById("m-change");
  const countEl = document.getElementById("m-sales-count");

  if (records.length === 0) {
    avgEl.innerHTML = '— <span class="unit">₽</span>';
    changeEl.textContent = "—";
    changeEl.style.color = "";
    countEl.textContent = "0";
    return;
  }

  const avg = records.reduce((s, r) => s + r.price, 0) / records.length;
  avgEl.innerHTML = `${Math.round(avg).toLocaleString("ru-RU")} <span class="unit">₽</span>`;
  countEl.textContent = records.length;

  const sorted = [...records].sort((a, b) => new Date(a.time) - new Date(b.time));
  const mid = Math.floor(sorted.length / 2);
  const firstHalf = sorted.slice(0, mid || 1);
  const secondHalf = sorted.slice(mid);
  const avgFirst = firstHalf.reduce((s, r) => s + r.price, 0) / firstHalf.length;
  const avgSecond = secondHalf.reduce((s, r) => s + r.price, 0) / secondHalf.length;

  if (firstHalf.length && secondHalf.length && avgFirst > 0) {
    const pct = ((avgSecond - avgFirst) / avgFirst) * 100;
    const sign = pct >= 0 ? "+" : "";
    changeEl.textContent = `${sign}${pct.toFixed(1)}%`;
    changeEl.style.color = pct >= 0 ? "#6fe0a0" : "#ff6b5c";
  } else {
    changeEl.textContent = "—";
    changeEl.style.color = "var(--muted-fg)";
  }
}

function renderSalesList(records) {
  const list = document.getElementById("m-sales-list");
  const head = document.getElementById("m-sales-head");
  const shownEl = document.getElementById("m-sales-shown");
  const moreWrap = document.getElementById("m-sales-more-wrap");

  if (records.length === 0) {
    list.innerHTML = '<div class="sales-empty">Нет продаж за выбранный период</div>';
    if (head) head.style.display = "none";
    if (moreWrap) moreWrap.style.display = "none";
    return;
  }

  const sorted = [...records].sort((a, b) => new Date(b.time) - new Date(a.time));
  const visible = sorted.slice(0, salesVisibleCount);
  if (head) {
    head.style.display = "flex";
    if (shownEl) shownEl.textContent = `Показано ${visible.length} из ${sorted.length}`;
  }
  // Кнопка скрыта по умолчанию — появится только при долистывании списка до конца
  if (moreWrap) moreWrap.style.display = "none";

  list.innerHTML = visible.map(r => {
    const rarity = RARITY[r.qlt ?? 0] || RARITY[0];
    const ptnLabel = r.ptn ? ` +${r.ptn}` : "";
    const time = new Date(r.time).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
    return `
      <div class="sale-row" style="--rarity-color:${rarity.color};--rarity-glow:${rarity.glow}">
        <div class="sale-left">
          <span class="sale-dot"></span>
          <span class="sale-rarity">${rarity.label}${ptnLabel}</span>
          <span class="sale-time">${time}</span>
        </div>
        <span class="sale-price">${r.price.toLocaleString("ru-RU")} ₽</span>
      </div>
    `;
  }).join("");

  // Проверяем, нужно ли показать кнопку «Показать ещё» после рендера
  updateSalesMoreButton();
}

function renderHistoryModal() {
  const chartEmpty = document.getElementById("m-chart-empty");
  const chart = document.getElementById("m-chart");
  const salesList = document.getElementById("m-sales-list");
  const avgEl = document.getElementById("m-avg-price");
  const changeEl = document.getElementById("m-change");
  const countEl = document.getElementById("m-sales-count");

  const moreWrap = document.getElementById("m-sales-more-wrap");

  if (!modalState.itemId) {
    chartEmpty.innerHTML = `<span class="chart-empty-icon">${ICONS.search}</span><span>Выберите артефакт</span>`;
    chartEmpty.style.display = "flex";
    chart.style.display = "none";
    salesList.innerHTML = '<div class="sales-empty">Выберите артефакт</div>';
    if (moreWrap) moreWrap.style.display = "none";
    avgEl.innerHTML = '— <span class="unit">₽</span>';
    changeEl.textContent = "—";
    changeEl.style.color = "";
    countEl.textContent = "0";
    return;
  }

  if (HISTORY_LOADING) {
    chartEmpty.innerHTML = `<span class="chart-empty-icon">${ICONS.clock}</span><span>загрузка истории…</span>`;
    chartEmpty.style.display = "flex";
    chart.style.display = "none";
    salesList.innerHTML = '<div class="sales-empty">Загрузка…</div>';
    if (moreWrap) moreWrap.style.display = "none";
    return;
  }

  updatePtnExactVisibility();
  const records = getFilteredHistory();
  renderChart(records);
  renderStats(records);
  renderSalesList(records);
}

init();
