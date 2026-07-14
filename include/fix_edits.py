import logging
import subprocess

from pydantic import BaseModel

log = logging.getLogger(__name__)


class CodeEdit(BaseModel):
    old: str
    new: str


class FixProposal(BaseModel):
    edits: list[CodeEdit]


def apply_edits(content: str, edits: list[dict]) -> str:
    if not edits:
        raise ValueError("Fix proposal contained no edits")
    for edit in edits:
        old = edit["old"]
        occurrences = content.count(old)
        if occurrences != 1:
            raise ValueError(
                f"Expected exactly one occurrence of the snippet to replace, "
                f"found {occurrences}: {old!r}"
            )
        content = content.replace(old, edit["new"], 1)
    return content


def ruff_format(content: str) -> str:
    try:
        proc = subprocess.run(
            ["ruff", "format", "-"],
            input=content,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        return proc.stdout
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        FileNotFoundError,
    ) as exc:
        log.warning("ruff format failed (%s); using unformatted content", exc)
        return content
