const runsList = document.getElementById("runs-list");
const runMeta = document.getElementById("run-meta");
const stepsBody = document.querySelector("#steps-table tbody");
const stepRowsMeta = document.getElementById("step-rows-meta");
const stepRowsTableHead = document.querySelector("#step-rows-table thead");
const stepRowsTableBody = document.querySelector("#step-rows-table tbody");
const refreshRunsBtn = document.getElementById("refresh-runs");
const dagGraph = document.getElementById("dag-graph");

let selectedRun = null;
let selectedStep = null;

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function renderRuns(runs) {
  runsList.innerHTML = "";
  for (const run of runs) {
    const item = document.createElement("li");
    item.textContent = `${run.run_name} (${run.dag_name})`;
    item.addEventListener("click", () => loadRun(run.run_name));
    runsList.appendChild(item);
  }
}

function renderRunMeta(run) {
  runMeta.textContent = JSON.stringify(
    {
      run_name: run.run_name,
      dag_name: run.dag_name,
      run_id: run.run_id,
      created_at_utc: run.created_at_utc,
      step_count: run.step_count,
    },
    null,
    2,
  );
}

function renderSteps(run) {
  stepsBody.innerHTML = "";
  for (const step of run.steps) {
    const tr = document.createElement("tr");
    if (selectedStep === step.step_name) {
      tr.classList.add("selected");
    }
    tr.innerHTML = `
      <td>${step.step_name}</td>
      <td>${step.status}</td>
      <td>${step.duration_ms ?? ""}</td>
      <td>${step.input_lines ?? ""}</td>
      <td>${step.output_lines ?? ""}</td>
      <td>${step.throughput_in_lps ?? ""}</td>
      <td>${step.throughput_out_lps ?? ""}</td>
    `;
    tr.addEventListener("click", () => selectStep(run.run_name, step.step_name));
    stepsBody.appendChild(tr);
  }
}

function statusClass(status) {
  if (status === "done") return "status-done";
  if (status === "failed") return "status-failed";
  if (status === "running") return "status-running";
  if (status === "skipped") return "status-skipped";
  return "status-pending";
}

function computeLayout(steps) {
  const map = new Map(steps.map((s) => [s.step_name, s]));
  const indegree = new Map();
  const downstream = new Map();
  for (const step of steps) {
    indegree.set(step.step_name, 0);
    downstream.set(step.step_name, []);
  }
  for (const step of steps) {
    for (const dep of step.deps || []) {
      if (!map.has(dep)) continue;
      indegree.set(step.step_name, (indegree.get(step.step_name) || 0) + 1);
      downstream.get(dep).push(step.step_name);
    }
  }
  const queue = [];
  for (const [name, deg] of indegree.entries()) {
    if (deg === 0) queue.push(name);
  }
  const levels = new Map();
  for (const name of queue) {
    levels.set(name, 0);
  }
  while (queue.length) {
    const node = queue.shift();
    const nextLevel = (levels.get(node) || 0) + 1;
    for (const child of downstream.get(node) || []) {
      indegree.set(child, (indegree.get(child) || 1) - 1);
      levels.set(child, Math.max(levels.get(child) || 0, nextLevel));
      if (indegree.get(child) === 0) {
        queue.push(child);
      }
    }
  }

  const grouped = new Map();
  for (const step of steps) {
    const level = levels.get(step.step_name) || 0;
    if (!grouped.has(level)) grouped.set(level, []);
    grouped.get(level).push(step.step_name);
  }

  const xGap = 220;
  const yGap = 110;
  const nodeW = 170;
  const nodeH = 52;
  const positions = new Map();
  let maxX = 0;
  let maxY = 0;
  const sortedLevels = [...grouped.keys()].sort((a, b) => a - b);
  for (const level of sortedLevels) {
    const nodes = grouped.get(level).sort();
    nodes.forEach((name, idx) => {
      const x = 40 + level * xGap;
      const y = 30 + idx * yGap;
      positions.set(name, { x, y, w: nodeW, h: nodeH });
      maxX = Math.max(maxX, x + nodeW + 40);
      maxY = Math.max(maxY, y + nodeH + 30);
    });
  }
  return { positions, width: Math.max(800, maxX), height: Math.max(240, maxY) };
}

function edgeStyle(step, dep) {
  if (step.disabled_deps && step.disabled_deps.includes(dep)) {
    return { stroke: "#9aa0a6", dash: "5 5" };
  }
  if (step.when_name && step.when_name === dep) {
    return {
      stroke: step.condition_bool === false ? "#c0392b" : "#1f8a4c",
      dash: "6 4",
    };
  }
  return { stroke: "#6b7280", dash: "" };
}

function renderGraph(run) {
  const steps = run.steps || [];
  dagGraph.innerHTML = "";
  if (!steps.length) {
    return;
  }
  const { positions, width, height } = computeLayout(steps);
  dagGraph.setAttribute("viewBox", `0 0 ${width} ${height}`);

  for (const step of steps) {
    const target = positions.get(step.step_name);
    for (const dep of step.deps || []) {
      const source = positions.get(dep);
      if (!source || !target) continue;
      const style = edgeStyle(step, dep);
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", String(source.x + source.w));
      line.setAttribute("y1", String(source.y + source.h / 2));
      line.setAttribute("x2", String(target.x));
      line.setAttribute("y2", String(target.y + target.h / 2));
      line.setAttribute("stroke", style.stroke);
      line.setAttribute("stroke-width", "2");
      if (style.dash) {
        line.setAttribute("stroke-dasharray", style.dash);
      }
      dagGraph.appendChild(line);
    }
  }

  for (const step of steps) {
    const pos = positions.get(step.step_name);
    if (!pos) continue;

    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.classList.add("graph-node");
    if (selectedStep === step.step_name) group.classList.add("selected");
    group.classList.add(statusClass(step.status));
    if (step.disabled_self) group.classList.add("disabled");
    group.addEventListener("click", () => selectStep(run.run_name, step.step_name));

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", String(pos.x));
    rect.setAttribute("y", String(pos.y));
    rect.setAttribute("rx", "8");
    rect.setAttribute("ry", "8");
    rect.setAttribute("width", String(pos.w));
    rect.setAttribute("height", String(pos.h));

    const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
    const deps = (step.deps || []).join(", ");
    title.textContent = `${step.step_name}\nstatus=${step.status}\ndeps=[${deps}]`;

    const name = document.createElementNS("http://www.w3.org/2000/svg", "text");
    name.setAttribute("x", String(pos.x + 10));
    name.setAttribute("y", String(pos.y + 22));
    name.textContent = step.step_name;

    const status = document.createElementNS("http://www.w3.org/2000/svg", "text");
    status.setAttribute("x", String(pos.x + 10));
    status.setAttribute("y", String(pos.y + 40));
    status.textContent = step.status;

    group.appendChild(title);
    group.appendChild(rect);
    group.appendChild(name);
    group.appendChild(status);
    dagGraph.appendChild(group);
  }
}

async function loadRun(runName) {
  const run = await fetchJson(`/api/runs/${runName}`);
  selectedRun = runName;
  if (selectedStep && !run.steps.some((s) => s.step_name === selectedStep)) {
    selectedStep = null;
  }
  renderRunMeta(run);
  renderGraph(run);
  renderSteps(run);
  if (selectedStep) {
    await loadStepRows(run.run_name, selectedStep);
  }
}

async function loadStepRows(runName, stepName) {
  const rows = await fetchJson(`/api/runs/${runName}/steps/${stepName}/rows?limit=100&offset=0`);
  renderStepRowsTable(stepName, rows);
}

async function selectStep(runName, stepName) {
  selectedStep = stepName;
  await loadRun(runName);
}

function renderStepRowsTable(stepName, page) {
  stepRowsMeta.textContent = `${stepName}: ${page.total_rows} rows (showing ${page.rows.length}, offset ${page.offset})`;
  stepRowsTableHead.innerHTML = "";
  stepRowsTableBody.innerHTML = "";

  const rows = page.rows || [];
  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="2">No rows</td>`;
    stepRowsTableBody.appendChild(tr);
    return;
  }

  const columnSet = new Set(["row_id"]);
  for (const row of rows) {
    const payload = row.payload;
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      for (const key of Object.keys(payload)) {
        columnSet.add(key);
      }
    } else {
      columnSet.add("payload");
    }
  }
  const columns = Array.from(columnSet);

  const headRow = document.createElement("tr");
  for (const col of columns) {
    const th = document.createElement("th");
    th.textContent = col;
    headRow.appendChild(th);
  }
  stepRowsTableHead.appendChild(headRow);

  for (const row of rows) {
    const tr = document.createElement("tr");
    const payload = row.payload;
    for (const col of columns) {
      const td = document.createElement("td");
      if (col === "row_id") {
        td.textContent = String(row.row_id ?? "");
      } else if (col === "payload") {
        td.textContent = JSON.stringify(payload);
      } else if (payload && typeof payload === "object" && !Array.isArray(payload)) {
        const value = payload[col];
        td.textContent = value === undefined ? "" : String(value);
      } else {
        td.textContent = "";
      }
      tr.appendChild(td);
    }
    stepRowsTableBody.appendChild(tr);
  }
}

async function loadRuns() {
  const runs = await fetchJson("/api/runs");
  renderRuns(runs);
  if (runs.length && !selectedRun) {
    await loadRun(runs[0].run_name);
  }
}

refreshRunsBtn.addEventListener("click", () => {
  loadRuns().catch((err) => {
    runMeta.textContent = String(err);
  });
});

setInterval(() => {
  if (selectedRun) {
    loadRun(selectedRun).catch(() => {});
  }
}, 2000);

loadRuns().catch((err) => {
  runMeta.textContent = String(err);
});
