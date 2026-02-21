from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from ninout.core.api.routes.runs import router as runs_router

app = FastAPI(title="ninout API", version="0.1.0")
app.include_router(runs_router)

_dashboard_dir = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "ui",
    "dashboard",
)
app.mount("/dashboard/assets", StaticFiles(directory=_dashboard_dir), name="dashboard_assets")


@app.get("/dashboard")
def dashboard() -> FileResponse:
    return FileResponse(os.path.join(_dashboard_dir, "index.html"))
