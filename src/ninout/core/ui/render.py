from __future__ import annotations

from typing import Mapping

from ninout.core.ui.layout import layout_positions
from ninout.core.engine.models import Step
from ninout.core.ui.serialize import load_yaml
from ninout.core.engine.validate import topological_order, validate_steps


def _esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def to_html_from_steps(steps: Mapping[str, Step], path: str = "dag.html") -> None:
    validate_steps(steps)
    positions, width, height = layout_positions(steps)

    node_w = 140
    node_h = 48

    edges_svg: list[str] = []
    for step in steps.values():
        for dep in step.deps:
            x1, y1 = positions[dep]
            x2, y2 = positions[step.name]
            x1 = x1 + node_w
            y1 = y1 + node_h // 2
            x2 = x2
            y2 = y2 + node_h // 2
            stroke = "#444"
            dash = ""
            if step.when == dep and step.condition is True:
                stroke = "#1f8a4c"
            elif step.when == dep and step.condition is False:
                stroke = "#c0392b"
                dash = "stroke-dasharray='6 4'"
            edges_svg.append(
                f"<line x1='{x1}' y1='{y1}' x2='{x2}' y2='{y2}' "
                f"stroke='{stroke}' stroke-width='2' {dash} marker-end='url(#arrow)' />"
            )

    nodes_svg: list[str] = []
    for name, (x, y) in positions.items():
        step = steps[name]
        fill = "#f5f2e9"
        if step.is_branch:
            fill = "#f7efe0"
        nodes_svg.append(
            f"<rect class='node' data-step='{_esc(name)}' x='{x}' y='{y}' "
            f"width='{node_w}' height='{node_h}' "
            f"rx='8' ry='8' fill='{fill}' stroke='#222' stroke-width='2' />"
        )
        nodes_svg.append(
            f"<text class='node-label' data-step='{_esc(name)}' "
            f"x='{x + node_w / 2}' y='{y + node_h / 2 + 5}' "
            "text-anchor='middle' font-family='Georgia, serif' "
            "font-size='14' fill='#111'>"
            f"{_esc(name)}"
            "</text>"
        )

    cards: list[str] = []
    ordered = topological_order(steps)
    for name in ordered:
        step = steps[name]
        status = step.status or "unknown"
        code = step.code or "# Code unavailable"
        result = step.result or ""
        duration = (
            f"{step.duration_ms:.3f} ms" if isinstance(step.duration_ms, float) else "n/a"
        )
        input_lines = (
            f"in {step.input_lines} lines"
            if isinstance(step.input_lines, int)
            else "in n/a"
        )
        output_lines = (
            f"out {step.output_lines} lines"
            if isinstance(step.output_lines, int)
            else "out n/a"
        )
        badge_class = "status"
        if status == "done":
            badge_class += " status-ok"
        elif status == "failed":
            badge_class += " status-fail"
        elif status == "skipped":
            badge_class += " status-skip"
        cards.append(
            f"""
        <article class='card' id='step-{_esc(step.name)}' hidden>
          <header>
            <div>
              <h2>{_esc(step.name)}</h2>
              <div class='meta'>
                <span class='{badge_class}'>{_esc(status)}</span>
                <span class='pill'>{'branch' if step.is_branch else 'step'}</span>
                <span class='pill'>{_esc(duration)}</span>
                <span class='pill'>{_esc(input_lines)}</span>
                <span class='pill'>{_esc(output_lines)}</span>
              </div>
            </div>
          </header>
          <div class='metrics'>
            <div><span>Input lines</span><strong>{_esc(str(step.input_lines) if step.input_lines is not None else "0")}</strong></div>
            <div><span>Output lines</span><strong>{_esc(str(step.output_lines) if step.output_lines is not None else "0")}</strong></div>
          </div>
          <div class='card-body'>
            <div>
              <h3>Code</h3>
              <pre><code class='language-python'>{_esc(code)}</code></pre>
            </div>
            <div>
              <h3>Preview</h3>
              <div class='preview-table' data-empty='true'></div>
              <pre class='result-raw'>{_esc(result or 'No result')}</pre>
            </div>
          </div>
        </article>
"""
        )

    html = f"""<!doctype html>
<html lang='pt-br'>
  <head>
    <meta charset='utf-8' />
    <meta name='viewport' content='width=device-width, initial-scale=1' />
    <title>DAG</title>
    <link rel='preconnect' href='https://fonts.googleapis.com'>
    <link rel='preconnect' href='https://fonts.gstatic.com' crossorigin>
    <link href='https://fonts.googleapis.com/css2?family=Spectral:wght@400;600&family=IBM+Plex+Mono:wght@400;600&display=swap' rel='stylesheet'>
    <link rel='stylesheet' href='https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/atom-one-light.min.css'>
    <style>
      body {{
        margin: 0;
        background: radial-gradient(circle at top, #f8f4ed, #eee4d4 55%, #e0d6c6);
        font-family: "Spectral", serif;
        color: #1c1c1c;
      }}
      .wrap {{
        padding: 28px;
        display: grid;
        gap: 28px;
      }}
      h1 {{
        margin: 0;
        font-size: 28px;
        letter-spacing: 0.5px;
        color: #222;
      }}
      svg {{
        background: #fffdf8;
        border: 2px solid #222;
        border-radius: 16px;
        box-shadow: 0 12px 30px rgba(0, 0, 0, 0.12);
        width: 100%;
        height: auto;
        max-width: 1200px;
        display: block;
      }}
      .node {{
        cursor: pointer;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
      }}
      .node:hover {{
        filter: drop-shadow(0 6px 8px rgba(0, 0, 0, 0.2));
      }}
      .node.selected {{
        stroke: #0b5ed7;
        stroke-width: 3px;
        filter: drop-shadow(0 10px 18px rgba(11, 94, 215, 0.35));
      }}
      .node-label {{
        pointer-events: none;
      }}
      .graph-card {{
        background: #fffaf1;
        border-radius: 18px;
        padding: 18px;
        box-shadow: 0 12px 24px rgba(0, 0, 0, 0.08);
        border: 1px solid rgba(0, 0, 0, 0.08);
      }}
      .cards {{
        display: grid;
        gap: 18px;
      }}
      .panel {{
        background: #ffffff;
        border-radius: 18px;
        padding: 18px;
        border: 1px solid rgba(0, 0, 0, 0.08);
        box-shadow: 0 12px 24px rgba(0, 0, 0, 0.08);
        min-height: 180px;
      }}
      .panel-placeholder {{
        color: #666;
        font-size: 14px;
      }}
      .card {{
        background: #ffffff;
        border-radius: 18px;
        padding: 18px;
        border: 1px solid rgba(0, 0, 0, 0.08);
        box-shadow: 0 8px 18px rgba(0, 0, 0, 0.06);
      }}
      .card header {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
        margin-bottom: 12px;
      }}
      .card h2 {{
        margin: 0;
        font-size: 20px;
      }}
      .meta {{
        display: flex;
        gap: 8px;
        align-items: center;
      }}
      .status {{
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        background: #e8e2d6;
      }}
      .status-ok {{
        background: #cfe8d7;
      }}
      .status-fail {{
        background: #f3c7c1;
      }}
      .status-skip {{
        background: #f2e5b4;
      }}
      .pill {{
        padding: 4px 10px;
        border-radius: 999px;
        background: #f2f2f2;
        font-size: 12px;
      }}
      .card-body {{
        display: grid;
        gap: 16px;
      }}
      .card h3 {{
        margin: 0 0 8px;
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}
      .metrics {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
        margin-bottom: 16px;
      }}
      .metrics div {{
        background: #f8f4ec;
        border-radius: 12px;
        padding: 10px 12px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 12px;
      }}
      .metrics span {{
        color: #555;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}
      .metrics strong {{
        font-size: 14px;
      }}
      pre {{
        margin: 0;
        padding: 12px;
        border-radius: 12px;
        background: #f7f4ec;
        overflow-x: auto;
        font-family: "IBM Plex Mono", monospace;
        font-size: 13px;
        line-height: 1.5;
      }}
      .preview-table {{
        display: none;
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid rgba(0, 0, 0, 0.1);
        background: #f7fbff;
      }}
      .preview-table table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
        font-family: "IBM Plex Mono", monospace;
      }}
      .preview-table th,
      .preview-table td {{
        padding: 8px 10px;
        border-bottom: 1px solid rgba(0, 0, 0, 0.08);
        text-align: left;
        vertical-align: top;
      }}
      .preview-table th {{
        background: #e8f0ff;
        font-weight: 600;
      }}
      pre.result-raw {{
        background: #f1f6ff;
      }}
      @media (min-width: 960px) {{
        .card-body {{
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }}
      }}
    </style>
  </head>
  <body>
    <div class='wrap'>
      <div>
        <h1>DAG Visualization</h1>
      </div>
      <section class='graph-card'>
        <svg viewBox='0 0 {width} {height}' xmlns='http://www.w3.org/2000/svg'>
          <defs>
            <marker id='arrow' markerWidth='10' markerHeight='10' refX='8' refY='3' orient='auto'>
              <path d='M0,0 L0,6 L9,3 z' fill='#444' />
            </marker>
          </defs>
          {''.join(edges_svg)}
          {''.join(nodes_svg)}
        </svg>
      </section>
      <section class='panel' id='detail-panel'>
        <div class='panel-placeholder'>Click a step to view details.</div>
        {''.join(cards)}
      </section>
    </div>
    <script src='https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js'></script>
    <script>
      const panel = document.getElementById('detail-panel');
      const cards = panel.querySelectorAll('.card');
      const esc = (value) => String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;');
      const showCard = (name) => {{
        cards.forEach(card => {{
          card.hidden = card.id !== `step-${{name}}`;
        }});
        document.querySelectorAll('.node').forEach(node => {{
          node.classList.toggle('selected', node.getAttribute('data-step') === name);
        }});
        const placeholder = panel.querySelector('.panel-placeholder');
        if (placeholder) placeholder.style.display = 'none';
        const card = panel.querySelector(`#step-${{name}}`);
        if (card) {{
          const raw = card.querySelector('.result-raw');
          const tableWrap = card.querySelector('.preview-table');
          if (raw && tableWrap) {{
            const text = raw.textContent.trim();
            let parsed = null;
            try {{
              parsed = JSON.parse(text);
            }} catch (_) {{
              parsed = null;
            }}
            if (parsed !== null && typeof parsed === 'object') {{
              const rows = Array.isArray(parsed) ? parsed : [parsed];
              const sample = rows;
              const keys = Array.isArray(parsed)
                ? Array.from(new Set(sample.flatMap(row => Object.keys(row || {{}}))))
                : Object.keys(parsed || {{}});
              if (!keys.length || !sample.length) {{
                tableWrap.style.display = 'none';
                raw.style.display = 'block';
              }} else {{
                const header = `<tr>${{keys.map(k => `<th>${{esc(k)}}</th>`).join('')}}</tr>`;
                const body = sample.map(row => {{
                  const cells = keys.map(k => {{
                    const value = row && typeof row === 'object' ? row[k] : row;
                    const rendered = typeof value === 'object' ? JSON.stringify(value) : value;
                    return `<td>${{esc(rendered)}}</td>`;
                  }}).join('');
                  return `<tr>${{cells}}</tr>`;
                }}).join('');
                tableWrap.innerHTML = `<table>${{header}}${{body}}</table>`;
                tableWrap.style.display = 'block';
                raw.style.display = 'none';
              }}
            }} else {{
              tableWrap.style.display = 'none';
              raw.style.display = 'block';
            }}
          }}
        }}
        hljs.highlightAll();
      }};
      document.querySelectorAll('.node, .node-label').forEach(node => {{
        node.addEventListener('click', () => {{
          const name = node.getAttribute('data-step');
          if (name) showCard(name);
        }});
      }});
    </script>
  </body>
</html>
"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def to_html_from_yaml(yaml_path: str, html_path: str = "dag.html") -> None:
    steps = load_yaml(yaml_path)
    to_html_from_steps(steps, path=html_path)
