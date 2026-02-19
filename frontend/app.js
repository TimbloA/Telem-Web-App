const BACKEND_URL = "https://timblo-telem-web-app.onrender.com";

function $(id) { return document.getElementById(id); }

function setStatus(msg) {
  $("status").textContent = msg || "";
}

function getPassword() {
  const pw = $("password").value;
  if (!pw) throw new Error("Password required.");
  return pw;
}

let clipboardFile = null;
let lastCrew = null;

/* ---------- FILE HANDLING ---------- */

function getFile() {
  if (clipboardFile) return clipboardFile;

  const f = $("csvFile").files?.[0];
  if (!f) throw new Error("Upload a CSV file or use Paste Data.");
  return f;
}

/* ---------- CLIPBOARD SUPPORT ---------- */

function detectDelimiter(text) {
  const firstLine = text.split(/\r?\n/)[0] || "";
  const tabCount = (firstLine.match(/\t/g) || []).length;
  const commaCount = (firstLine.match(/,/g) || []).length;
  return (tabCount > commaCount) ? "\t" : ",";
}

function escapeCsvField(value) {
  const s = String(value ?? "");
  if (/[",\r\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function tsvToCsv(text) {
  const lines = text.split(/\r?\n/);
  return lines.map(line =>
    line.split("\t").map(escapeCsvField).join(",")
  ).join("\n");
}

async function fileFromClipboard() {
  const text = await navigator.clipboard.readText();
  if (!text.trim()) throw new Error("Clipboard empty.");

  const delimiter = detectDelimiter(text);
  const csvText = delimiter === "\t" ? tsvToCsv(text) : text;

  const blob = new Blob([csvText], { type: "text/csv" });
  return new File([blob], "clipboard.csv", { type: "text/csv" });
}

$("btnPaste").addEventListener("click", async () => {
  try {
    clipboardFile = await fileFromClipboard();
    lastCrew = null;
    setStatus("Clipboard data loaded. Click Preview Crew.");
  } catch (e) {
    setStatus(e.message);
  }
});

/* ---------- WEIGHTS UI ---------- */

function renderWeights(crew) {
  const wrap = $("weightsTable");
  wrap.innerHTML = "";

  crew.forEach(row => {
    const div = document.createElement("div");
    div.className = "wrow";
    div.innerHTML = `
      <div>${row.pos}</div>
      <div>${row.name}</div>
      <div>${row.abbr}</div>
      <div><input id="w_${row.pos}" type="number" step="0.1" min="0"></div>
    `;
    wrap.appendChild(div);
  });
}

function collectWeights() {
  const weights = {};
  for (let i = 1; i <= 8; i++) {
    const val = $(`w_${i}`)?.value?.trim();
    if (!val) throw new Error(`Missing weight for seat ${i}`);
    weights[`pos_${i}`] = val;
  }
  return weights;
}

/* ---------- PREVIEW CREW ---------- */

$("btnPreview").addEventListener("click", async () => {
  try {
    setStatus("Loading crew...");
    const pw = getPassword();
    const file = getFile();

    const fd = new FormData();
    fd.append("file", file);

    const res = await fetch(`${BACKEND_URL}/preview-crew`, {
      method: "POST",
      headers: { "X-C150-Password": pw },
      body: fd
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.detail);

    lastCrew = data.crew;
    renderWeights(lastCrew);
    setStatus("Crew loaded. Enter weights.");
  } catch (e) {
    setStatus(e.message);
  }
});

/* ---------- PROCESS + DOWNLOAD ---------- */

$("btnProcess").addEventListener("click", async () => {
  try {
    setStatus("Processing...");
    const pw = getPassword();
    const file = getFile();
    if (!lastCrew) throw new Error("Preview crew first.");

    const weights = collectWeights();

    const fd = new FormData();
    fd.append("file", file);
    fd.append("season", $("season").value);
    fd.append("shell", $("shell").value);
    fd.append("zone", $("zone").value);
    fd.append("piece", $("piece").value);
    fd.append("piece_number", $("pieceNumber").value);
    fd.append("cox_uni", $("coxUni").value);
    fd.append("rig_info", $("rigInfo").value);
    fd.append("wind", $("wind").value);
    fd.append("stream", $("stream").value);
    fd.append("temperature", $("temp").value);
    fd.append("weights_json", JSON.stringify(weights));

    const res = await fetch(`${BACKEND_URL}/process`, {
      method: "POST",
      headers: { "X-C150-Password": pw },
      body: fd
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail);
    }

    const blob = await res.blob();
    const dispo = res.headers.get("Content-Disposition") || "";
    const match = dispo.match(/filename="(.+?)"/);
    const filename = match ? match[1] : "C150_processed.csv";

    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);

    setStatus(`Done. Downloaded: ${filename}`);
  } catch (e) {
    setStatus(e.message);
  }
});
