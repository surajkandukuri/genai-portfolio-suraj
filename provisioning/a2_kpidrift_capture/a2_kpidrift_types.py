from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict

@dataclass
class Artifacts:
    full: Path
    report: Path
    html: Optional[Path] = None
    log: Optional[Path] = None

@dataclass
class CaptureResult:
    provider: str
    url: str
    outdir: Path
    artifacts: Artifacts
    meta: Dict[str, str]
