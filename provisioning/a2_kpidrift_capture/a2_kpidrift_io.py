import json, hashlib
from dataclasses import asdict
from pathlib import Path
from .a2_kpidrift_types import CaptureResult

def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def write_sidecar(result: CaptureResult) -> Path:
    sidecar = result.outdir / f"{result.provider}_capture_{result.artifacts.full.stem[-14:]}.json"
    payload = {
        "provider": result.provider,
        "url": result.url,
        "outdir": str(result.outdir),
        "artifacts": {
            "full": str(result.artifacts.full),
            "report": str(result.artifacts.report),
            "html": str(result.artifacts.html) if result.artifacts.html else None,
            "log": str(result.artifacts.log) if result.artifacts.log else None,
        },
        "hashes": {
            "full_sha256": _sha256(result.artifacts.full) if result.artifacts.full.exists() else None,
            "report_sha256": _sha256(result.artifacts.report) if result.artifacts.report.exists() else None,
        },
        "meta": result.meta,
    }
    sidecar.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return sidecar
