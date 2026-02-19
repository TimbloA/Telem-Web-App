function $(id) { return document.getElementById(id); }

function setStatus(msg) {
  $("status").textContent = msg || "";
}

function getBackend() {
  const url = $("backendUrl").value.trim().replace(/\/+$/, "");
  if (!url) throw new Error("Backend URL is required.");
  return url;
}

function getPassword() {
  const pw = $("password").value;
  if (!pw) throw new Error("Password is required.");
  return pw;
}

function getFile() {
  const f = $("csvFile").files?.[0];
  if (!f) throw new Error("CSV file is required.");
  return f;
}

function renderWeights(crew) {
  const wrap = $("weightsTable");
  wrap.innerHTML = "";

  const hdr = document.createElement("div");
  hdr.className = "wrow hdr";
  hdr.innerHTML = `<div>Pos</div><div>Name</div><div>Abbr/UNI</div><div>Weight (kg)</div>`;
  wrap.appendChild(hdr);

  crew.forEach(row => {
    const div = document.createElement("div");
    div.className = "wrow";
    div.dataset.pos = row.pos;

    const inputId = `w_pos_${row.pos}`;
    div.innerHTML = `
      <div>${row.pos}</div>
      <div>${row.name || ""}</div>
      <div>${row.abbr || ""}</div>
      <div><input id="${inputId}" type="number" step="0.1" min="0" placeholder="kg" value="${row.existing_weight || ""}"></div>
    `;
    wrap.appendChild(div);
  });
}

function collectWeights(crew) {
  // We send seat keys pos_1..pos_8 ALWAYS.
  // Also send abbr keys if present, so backend can write by abbr when possible.
  const out = {};
  crew.forEach(row => {
    const raw = $(`w_pos_${row.pos}`).value.trim();
    out[`pos_${row.pos}`] = raw;
    if (row.abbr && row.abbr.trim()) {
      out[row.abbr.trim().toLowerCase()] = raw;
    }
  });
  return out;
}

let lastCrew = null;

$("btnPreview").addEventListener("click", async () => {
  try {
    setStatus("");
    const backend = getBackend();
    const pw = getPassword();
    const file = getFile();

    const fd = new FormData();
    fd.append("file", file);

    setStatus("Reading crew info…");
    const res = await fetch(`${backend}/preview-crew`, {
      method: "POST",
      headers: { "X-C150-Password": pw },
      body: fd
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "Preview failed.");

    lastCrew = data.crew;
    renderWeights(lastCrew);
    setStatus("Crew loaded. Enter weights (kg) for seats 1–8.");
  } catch (e) {
    setStatus(e.message || String(e));
  }
});

$("btnProcess").addEventListener("click", async () => {
  try {
    setStatus("");
    const backend = getBackend();
    const pw = getPassword();
    const file = getFile();

    if (!lastCrew) throw new Error("Click “Preview Crew (Weights)” first.");

    const weightsObj = collectWeights(lastCrew);

    // Basic frontend checks for weights
    for (let seat = 1; seat <= 8; seat++) {
      const raw = (weightsObj[`pos_${seat}`] || "").trim();
      if (!raw) throw new Error(`Missing weight for seat ${seat} (kg).`);
      const val = Number(raw);
      if (!Number.isFinite(val) || val <= 0) throw new Error(`Invalid weight for seat ${seat} (kg).`);
    }

    const season = $("season").value.trim() || "FY26";
    const shell = $("shell").value.trim().toUpperCase();
    const zone = $("zone").value.trim().toUpperCase();
    const piece = $("piece").value.trim();
    const pieceNumber = $("pieceNumber").value.trim();

    const coxUni = $("coxUni").value.trim();
    const rigInfo = $("rigInfo").value.trim();

    const wind = $("wind").value.trim();
    const stream = $("stream").value.trim();
    const temp = $("temp").value.trim();

    if (!shell) throw new Error("Shell is required.");
    if (!piece) throw new Error("Piece is required.");
    if (!pieceNumber) throw new Error("Piece Number is required.");
    if (!coxUni) throw new Error("Cox UNI is required.");
    if (!rigInfo) throw new Error("Rig Info is required.");

    const fd = new FormData();
    fd.append("file", file);

    fd.append("season", season);
    fd.append("shell", shell);
    fd.append("zone", zone);
    fd.append("piece", piece);
    fd.append("piece_number", pieceNumber);

    fd.append("cox_uni", coxUni);
    fd.append("rig_info", rigInfo);

    fd.append("wind", wind);
    fd.append("stream", stream);
    fd.append("temperature", temp);

    fd.append("weights_json", JSON.stringify(weightsObj));

    setStatus("Processing…");
    const res = await fetch(`${backend}/process`, {
      method: "POST",
      headers: { "X-C150-Password": pw },
      body: fd
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || "Processing failed.");
    }

    const blob = await res.blob();
    const dispo = res.headers.get("Content-Disposition") || "";
    const match = dispo.match(/filename=\"(.+?)\"/);
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
    setStatus(e.message || String(e));
  }
});
