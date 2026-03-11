const state = {
  logs: [],
  empresas: [],
  columns: [],
  logFilter: "Todos",
  loadingEmpresas: false,
  sortKey: "",
  sortDir: "asc",
  flow: {
    steps: [],
    groups: {},
    selection: {},
  },
};

const months = [
  { value: "01", label: "Janeiro" },
  { value: "02", label: "Fevereiro" },
  { value: "03", label: "Marco" },
  { value: "04", label: "Abril" },
  { value: "05", label: "Maio" },
  { value: "06", label: "Junho" },
  { value: "07", label: "Julho" },
  { value: "08", label: "Agosto" },
  { value: "09", label: "Setembro" },
  { value: "10", label: "Outubro" },
  { value: "11", label: "Novembro" },
  { value: "12", label: "Dezembro" },
];

const api = async (url, options = {}) => {
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  return res.json();
};

const el = (id) => document.getElementById(id);

const getFilenameFromDisposition = (disposition, fallback) => {
  const raw = String(disposition || "");
  const utf8Match = raw.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match && utf8Match[1]) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch (err) {
      // ignore and try fallback parser
    }
  }
  const plainMatch = raw.match(/filename="?([^\";]+)"?/i);
  if (plainMatch && plainMatch[1]) {
    return plainMatch[1];
  }
  return fallback;
};

const downloadBlobFallback = (blob, fileName) => {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
};

const saveBlobWithPicker = async (blob, fileName) => {
  if (typeof window.showSaveFilePicker !== "function") return false;
  const handle = await window.showSaveFilePicker({
    suggestedName: fileName,
    types: [{ description: "Arquivo ZIP", accept: { "application/zip": [".zip"] } }],
  });
  const writable = await handle.createWritable();
  await writable.write(blob);
  await writable.close();
  return true;
};

const fmtDuration = (secs) => {
  const s = Math.max(0, parseInt(secs || 0, 10));
  const h = String(Math.floor(s / 3600)).padStart(2, "0");
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, "0");
  const r = String(s % 60).padStart(2, "0");
  return `${h}:${m}:${r}`;
};

const normalizeColumnLabel = (col) => {
  const raw = String(col || "");
  const lower = raw.toLowerCase();
  const map = {
    empresa: "Empresa",
    cnpj: "CNPJ",
    situacao: "Situacao",
    ultimo_status: "Ultimo status",
    processado_em: "Processado em",
    ultimo_erro: "Ultimo erro",
  };
  if (map[lower]) return map[lower];
  if (raw === "Sel") return "Sel";
  const spaced = lower.replace(/_/g, " ");
  return spaced.replace(/\b\w/g, (m) => m.toUpperCase());
};

const sortValue = (row, key) => {
  const raw = getCellValue(row, key);
  if (key && String(key).toLowerCase() === "cnpj") {
    return String(raw || "").replace(/\D/g, "");
  }
  if (raw === null || raw === undefined) return "";
  const str = String(raw).trim();
  if (!str) return "";
  if (/^\d{4}-\d{2}-\d{2}/.test(str)) {
    const parsed = Date.parse(str.replace(" ", "T"));
    if (!Number.isNaN(parsed)) return parsed;
  }
  if (/^-?\d+([.,]\d+)?$/.test(str)) {
    const num = parseFloat(str.replace(",", "."));
    if (!Number.isNaN(num)) return num;
  }
  return str.toLowerCase();
};

const compareValues = (a, b) => {
  if (a === b) return 0;
  if (a === "" && b !== "") return 1;
  if (a !== "" && b === "") return -1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b), "pt-BR", { numeric: true, sensitivity: "base" });
};

const setSort = (col) => {
  if (col === "Sel") return;
  if (state.sortKey === col) {
    state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
  } else {
    state.sortKey = col;
    state.sortDir = "asc";
  }
  renderEmpresas();
};

const updateMeta = (s) => {
  const browserEl = el("browser-state");
  const lastRunEl = el("last-run");
  const lastResultEl = el("last-result");
  if (browserEl) {
    browserEl.textContent = s.browser_open ? "Aberto" : "Fechado";
  }

  const last = s.last_run || {};
  if (lastRunEl) {
    lastRunEl.textContent = last.at ? last.at : "-";
  }

  if (lastResultEl) {
    let resultText = "-";
    let resultClass = "";
    if (last && last.at) {
      const proc = Number(last.processed || 0);
      const errs = Number(last.errors || 0);
      const dur = fmtDuration(last.duration || 0);
      if (last.ok === true) {
        resultText = `Sucesso (Proc: ${proc}, Erros: ${errs}, ${dur})`;
        resultClass = "success";
      } else if (last.ok === false) {
        resultText = `Falha (Proc: ${proc}, Erros: ${errs}, ${dur})`;
        resultClass = "error";
      } else {
        resultText = `Finalizado (Proc: ${proc}, Erros: ${errs}, ${dur})`;
        resultClass = "warn";
      }
    }
    lastResultEl.textContent = resultText;
    lastResultEl.className = resultClass ? `meta-value ${resultClass}` : "meta-value";
  }
};

const setupTabs = () => {
  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
      document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
      btn.classList.add("active");
      const target = btn.dataset.tab;
      el(target).classList.add("active");
    });
  });
};

const fillPeriodSelectors = () => {
  const mesDe = el("mes-de");
  const mesAte = el("mes-ate");
  const anoDe = el("ano-de");
  const anoAte = el("ano-ate");
  months.forEach((m) => {
    mesDe.append(new Option(m.label, m.value));
    mesAte.append(new Option(m.label, m.value));
  });
  const currentYear = new Date().getFullYear();
  for (let y = 2010; y <= currentYear; y += 1) {
    anoDe.append(new Option(String(y), String(y)));
    anoAte.append(new Option(String(y), String(y)));
  }
};

const loadPeriod = async () => {
  const data = await api("/api/period");
  el("mes-de").value = String(data.mes_de).padStart(2, "0");
  el("mes-ate").value = String(data.mes_ate).padStart(2, "0");
  el("ano-de").value = String(data.ano_de);
  el("ano-ate").value = String(data.ano_ate);
};

const savePeriod = async () => {
  const payload = {
    mes_de: el("mes-de").value,
    ano_de: el("ano-de").value,
    mes_ate: el("mes-ate").value,
    ano_ate: el("ano-ate").value,
  };
  await api("/api/period", { method: "POST", body: JSON.stringify(payload) });
};

const updateStats = (s) => {
  el("stat-total").textContent = s.total;
  el("stat-processed").textContent = s.processed;
  el("stat-errors").textContent = s.errors;
  el("stat-waiting").textContent = s.waiting;
  el("stat-selected").textContent = s.selected;
  el("run-status").textContent = `Status: ${s.status_text}`;
  el("timer").textContent = `Tempo: ${fmtDuration(s.elapsed)}`;
  el("btn-start").disabled = s.running;
  el("btn-stop").disabled = !s.running;
  el("btn-close").disabled = !s.browser_open;
  el("btn-load-emp").disabled = s.loading_empresas || s.running;

  const pill = el("run-pill");
  pill.textContent = s.running ? "Em execucao" : "Aguardando";
  updateMeta(s);

  if (state.loadingEmpresas && !s.loading_empresas) {
    state.loadingEmpresas = false;
    loadEmpresas();
  } else {
    state.loadingEmpresas = s.loading_empresas;
  }
};

const pollStatus = async () => {
  const s = await api("/api/status");
  updateStats(s);
};

const renderLogEntry = (entry) => {
  const filter = state.logFilter;
  if (
    (filter === "Sucessos" && entry.level !== "SUCCESS") ||
    (filter === "Avisos" && entry.level !== "WARN") ||
    (filter === "Erros" && entry.level !== "ERROR")
  ) {
    return;
  }
  const logBox = el("log-box");
  const line = document.createElement("div");
  line.className = "log-line";
  line.innerHTML = `<span class="ts">${entry.ts}</span><span class="level ${entry.level}">${entry.level}</span><span>${entry.message}</span>`;
  logBox.appendChild(line);
  logBox.scrollTop = logBox.scrollHeight;
};

const pushLog = (entry) => {
  state.logs.push(entry);
  if (state.logs.length > 5000) state.logs.shift();
  renderLogEntry(entry);
};

const renderLogs = () => {
  const logBox = el("log-box");
  logBox.innerHTML = "";
  state.logs.forEach((entry) => renderLogEntry(entry));
};

let logCursor = 0;
let logPollTimer = null;
let logWs = null;

const startLogPolling = () => {
  if (logPollTimer) return;
  logPollTimer = setInterval(async () => {
    try {
      const res = await api(`/api/logs?since=${logCursor}`);
      const items = res.items || [];
      items.forEach((entry) => pushLog(entry));
      if (typeof res.next === "number") logCursor = res.next;
    } catch (err) {
      // ignore
    }
  }, 1000);
};

const stopLogPolling = () => {
  if (logPollTimer) {
    clearInterval(logPollTimer);
    logPollTimer = null;
  }
};

const connectLogs = () => {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  logWs = new WebSocket(`${proto}://${window.location.host}/ws/logs`);
  logWs.onopen = () => stopLogPolling();
  logWs.onmessage = (event) => {
    const entry = JSON.parse(event.data);
    pushLog(entry);
    logCursor += 1;
  };
  logWs.onerror = () => startLogPolling();
  logWs.onclose = () => startLogPolling();
};

const loadConfig = async () => {
  const cfg = await api("/api/config");
  el("cfg-downloads").value = cfg.downloads_dir || "";
  el("cfg-close-after").checked = !!cfg.close_browser_after;
  el("cfg-headless").checked = !!cfg.headless;
  el("cfg-manual-login").checked = !!cfg.manual_login;
  const creds = cfg.credentials || {};
  el("cfg-user").value = creds.username || "";
  el("cfg-pass").value = creds.password || "";
};

const normalizeFlowSelection = (steps, selection) => {
  const out = {};
  (steps || []).forEach((step) => {
    const key = String(step.key || "");
    if (!key) return;
    out[key] = selection && Object.prototype.hasOwnProperty.call(selection, key) ? !!selection[key] : true;
  });
  return out;
};

const renderFlow = () => {
  const root = el("flow-groups");
  if (!root) return;
  root.innerHTML = "";

  const steps = state.flow.steps || [];
  if (!steps.length) {
    const empty = document.createElement("div");
    empty.className = "hint";
    empty.textContent = "Nenhuma etapa disponivel.";
    root.appendChild(empty);
    return;
  }

  const groups = state.flow.groups || {};
  const grouped = {};
  steps.forEach((step) => {
    const group = String(step.group || "geral");
    if (!grouped[group]) grouped[group] = [];
    grouped[group].push(step);
  });

  Object.keys(grouped).forEach((groupKey) => {
    const box = document.createElement("div");
    box.className = "flow-group";

    const title = document.createElement("div");
    title.className = "flow-group-title";
    title.textContent = groups[groupKey] || groupKey;
    box.appendChild(title);

    grouped[groupKey].forEach((step) => {
      const key = String(step.key || "");
      if (!key) return;

      const label = document.createElement("label");
      label.className = "checkbox";

      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = !!state.flow.selection[key];
      cb.addEventListener("change", () => {
        state.flow.selection[key] = cb.checked;
      });

      const txt = document.createElement("span");
      txt.textContent = step.label || key;

      label.appendChild(cb);
      label.appendChild(txt);
      box.appendChild(label);
    });

    root.appendChild(box);
  });
};

const loadFlow = async () => {
  const data = await api("/api/flow");
  const steps = Array.isArray(data.steps) ? data.steps : [];
  state.flow.steps = steps;
  state.flow.groups = data.groups || {};
  state.flow.selection = normalizeFlowSelection(steps, data.selection || {});
  renderFlow();
};

const saveFlow = async () => {
  const selection = normalizeFlowSelection(state.flow.steps || [], state.flow.selection || {});
  state.flow.selection = selection;
  const res = await api("/api/flow", { method: "POST", body: JSON.stringify({ selection }) });
  if (res && res.selection) {
    state.flow.selection = normalizeFlowSelection(state.flow.steps || [], res.selection);
  }
  renderFlow();
};

const saveConfig = async () => {
  const payload = {
    downloads_dir: el("cfg-downloads").value.trim(),
    close_browser_after: el("cfg-close-after").checked,
    headless: el("cfg-headless").checked,
    manual_login: el("cfg-manual-login").checked,
    credentials: {
      username: el("cfg-user").value.trim(),
      password: el("cfg-pass").value,
    },
  };
  await api("/api/config", { method: "POST", body: JSON.stringify(payload) });
};

const exportDownloadsToComputer = async () => {
  const btn = el("btn-export-downloads");
  if (btn) btn.disabled = true;
  try {
    const res = await fetch("/api/downloads/archive");
    if (!res.ok) {
      let msg = "Falha ao preparar downloads.";
      try {
        const err = await res.json();
        msg = err.detail || err.error || msg;
      } catch (e) {
        // ignore
      }
      alert(msg);
      return;
    }
    const blob = await res.blob();
    const fileName = getFilenameFromDisposition(
      res.headers.get("content-disposition"),
      `downloads_nota_manaus_${Date.now()}.zip`,
    );

    try {
      const saved = await saveBlobWithPicker(blob, fileName);
      if (!saved) downloadBlobFallback(blob, fileName);
    } catch (err) {
      if (err && err.name === "AbortError") return;
      downloadBlobFallback(blob, fileName);
    }
  } catch (err) {
    alert(`Falha ao baixar downloads: ${err}`);
  } finally {
    if (btn) btn.disabled = false;
  }
};

const loadScheduler = async () => {
  const sched = await api("/api/scheduler");
  el("sched-next").textContent = sched.next_run
    ? `Proxima execucao: ${sched.next_run}${sched.recurring ? " (recorrente)" : ""}`
    : "Proxima execucao: -";
  el("sched-date").value = sched.date || "";
  el("sched-time").value = sched.time || "";
  el("sched-rec").checked = !!sched.recurring;
  el("sched-enabled").checked = !!sched.enabled;
};

const saveScheduler = async (enabledOverride) => {
  const enabled = enabledOverride !== undefined ? enabledOverride : el("sched-enabled").checked;
  const payload = {
    enabled,
    recurring: el("sched-rec").checked,
    date: el("sched-date").value,
    time: el("sched-time").value,
  };
  await api("/api/scheduler", { method: "POST", body: JSON.stringify(payload) });
  loadScheduler();
};

const getCellValue = (row, col) => {
  if (col === "Sel") return "";
  if (row[col] !== undefined) return row[col];
  const lower = col.toLowerCase();
  if (row[lower] !== undefined) return row[lower];
  if (lower === "empresa") return row.Empresa || "";
  return "";
};

const renderEmpresas = () => {
  const filter = (el("emp-search").value || "").toLowerCase().trim();
  const head = el("emp-head");
  const body = el("emp-body");
  head.innerHTML = "";
  body.innerHTML = "";

  const columns = state.columns.length ? state.columns : ["Sel", "empresa", "cnpj", "situacao"];
  if (!columns.includes(state.sortKey)) {
    state.sortKey =
      columns.find((col) => String(col).toLowerCase() === "empresa") ||
      columns.find((col) => col !== "Sel") ||
      "";
    state.sortDir = "asc";
  }
  const trHead = document.createElement("tr");
  columns.forEach((col, idx) => {
    const th = document.createElement("th");
    const sortable = col !== "Sel";
    if (sortable) th.classList.add("sortable");
    if (idx === 0) th.classList.add("sticky-0", "col-sel");
    if (idx === 1) th.classList.add("sticky-1", "col-empresa");
    const inner = document.createElement("div");
    inner.className = "th-inner";
    const label = document.createElement("span");
    label.textContent = normalizeColumnLabel(col);
    inner.appendChild(label);
    if (sortable) {
      const indicator = document.createElement("span");
      indicator.className = "sort-indicator";
      if (state.sortKey === col) {
        indicator.textContent = state.sortDir === "asc" ? "^" : "v";
      }
      inner.appendChild(indicator);
      th.addEventListener("click", () => setSort(col));
    }
    th.appendChild(inner);
    trHead.appendChild(th);
  });
  head.appendChild(trHead);

  let items = state.empresas.filter((row) => {
    if (!filter) return true;
    const nome = String(row.empresa || row.Empresa || "").toLowerCase();
    const cnpj = String(row.cnpj || "").toLowerCase();
    return nome.includes(filter) || cnpj.includes(filter);
  });

  if (state.sortKey) {
    const dir = state.sortDir === "desc" ? -1 : 1;
    const key = state.sortKey;
    items = [...items].sort((a, b) => dir * compareValues(sortValue(a, key), sortValue(b, key)));
  }

  items.forEach((row) => {
    const tr = document.createElement("tr");
    if (row.Sel) tr.classList.add("selected-row");
    columns.forEach((col, idx) => {
      const td = document.createElement("td");
      if (idx === 0) td.classList.add("sticky-0", "col-sel");
      if (idx === 1) td.classList.add("sticky-1", "col-empresa");
      if (col === "Sel") {
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.checked = !!row.Sel;
        cb.addEventListener("change", async () => {
          row.Sel = cb.checked;
          tr.classList.toggle("selected-row", cb.checked);
          await api("/api/empresas/mark", {
            method: "POST",
            body: JSON.stringify({
              updates: [
                { cnpj: row.cnpj || "", empresa: row.empresa || row.Empresa || "", Sel: cb.checked },
              ],
            }),
          });
          pollStatus();
        });
        td.appendChild(cb);
      } else {
        td.textContent = getCellValue(row, col);
      }
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });

  el("empresas-status").textContent = `Mostrando ${items.length} de ${state.empresas.length} empresas`;
};

const loadEmpresas = async () => {
  const data = await api("/api/empresas");
  state.columns = data.columns || [];
  state.empresas = data.items || [];
  if (!state.sortKey || !state.columns.includes(state.sortKey)) {
    state.sortKey =
      state.columns.find((col) => String(col).toLowerCase() === "empresa") ||
      state.columns.find((col) => col !== "Sel") ||
      "";
    state.sortDir = "asc";
  }
  renderEmpresas();
};

const bindEvents = () => {
  el("btn-start").addEventListener("click", async () => {
    await savePeriod();
    await saveFlow();
    await api("/api/start", { method: "POST" });
  });
  el("btn-stop").addEventListener("click", async () => {
    await api("/api/stop", { method: "POST" });
  });
  el("btn-close").addEventListener("click", async () => {
    await api("/api/close-browser", { method: "POST" });
  });

  el("log-filter").addEventListener("change", (e) => {
    state.logFilter = e.target.value;
    renderLogs();
  });

  ["mes-de", "mes-ate", "ano-de", "ano-ate"].forEach((id) => {
    el(id).addEventListener("change", () => savePeriod());
  });

  el("btn-save-config").addEventListener("click", async () => {
    await saveConfig();
    await saveFlow();
  });
  el("btn-export-downloads").addEventListener("click", exportDownloadsToComputer);
  el("btn-toggle-pass").addEventListener("click", () => {
    const pass = el("cfg-pass");
    pass.type = pass.type === "password" ? "text" : "password";
    el("btn-toggle-pass").textContent = pass.type === "password" ? "Mostrar" : "Ocultar";
  });

  el("btn-sched-save").addEventListener("click", () => saveScheduler());
  el("btn-sched-cancel").addEventListener("click", () => saveScheduler(false));

  el("btn-load-emp").addEventListener("click", async () => {
    const res = await api("/api/empresas/load", { method: "POST" });
    if (res.ok) state.loadingEmpresas = true;
  });
  el("btn-mark-all").addEventListener("click", async () => {
    await api("/api/empresas/mark-all", { method: "POST", body: JSON.stringify({ value: true }) });
    await loadEmpresas();
  });
  el("btn-unmark-all").addEventListener("click", async () => {
    await api("/api/empresas/mark-all", { method: "POST", body: JSON.stringify({ value: false }) });
    await loadEmpresas();
  });

  el("btn-flow-all").addEventListener("click", async () => {
    (state.flow.steps || []).forEach((step) => {
      const key = String(step.key || "");
      if (key) state.flow.selection[key] = true;
    });
    renderFlow();
    await saveFlow();
  });

  el("btn-flow-none").addEventListener("click", async () => {
    (state.flow.steps || []).forEach((step) => {
      const key = String(step.key || "");
      if (key) state.flow.selection[key] = false;
    });
    renderFlow();
    await saveFlow();
  });

  el("emp-search").addEventListener("input", renderEmpresas);
  el("btn-clear-search").addEventListener("click", () => {
    el("emp-search").value = "";
    renderEmpresas();
  });
};

const init = async () => {
  setupTabs();
  fillPeriodSelectors();
  bindEvents();
  connectLogs();
  await loadConfig();
  await loadFlow();
  await loadPeriod();
  await loadScheduler();
  await loadEmpresas();
  await pollStatus();
  setInterval(pollStatus, 1000);
};

window.addEventListener("DOMContentLoaded", init);
