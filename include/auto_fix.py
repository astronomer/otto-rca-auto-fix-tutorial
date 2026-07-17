import base64
import json
import logging
import os
import re
import subprocess
from collections.abc import Iterator

import requests
from pydantic import BaseModel

log = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
ASTRO_API_BASE = "https://api.astronomer.io/labs/v1"


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


def github_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def find_open_pr(repo: str, label: str) -> dict | None:
    resp = requests.get(
        f"{GITHUB_API}/repos/{repo}/pulls",
        params={"state": "open", "labels": label},
        headers=github_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    prs = resp.json()
    return prs[0] if prs else None


def get_repo_file(repo: str, ref: str, path: str) -> dict[str, str]:
    resp = requests.get(
        f"{GITHUB_API}/repos/{repo}/contents/{path}",
        params={"ref": ref},
        headers=github_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    return {
        "content": base64.b64decode(body["content"]).decode("utf-8"),
        "sha": body["sha"],
    }


def create_branch(repo: str, base_branch: str, branch: str) -> None:
    base_ref = requests.get(
        f"{GITHUB_API}/repos/{repo}/git/ref/heads/{base_branch}",
        headers=github_headers(),
        timeout=15,
    )
    base_ref.raise_for_status()
    base_sha = base_ref.json()["object"]["sha"]

    resp = requests.post(
        f"{GITHUB_API}/repos/{repo}/git/refs",
        headers=github_headers(),
        json={"ref": f"refs/heads/{branch}", "sha": base_sha},
        timeout=15,
    )
    resp.raise_for_status()


def commit_file(
    repo: str, branch: str, path: str, content: str, sha: str, message: str
) -> None:
    resp = requests.put(
        f"{GITHUB_API}/repos/{repo}/contents/{path}",
        headers=github_headers(),
        json={
            "message": message,
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "sha": sha,
            "branch": branch,
        },
        timeout=30,
    )
    resp.raise_for_status()


def create_pull_request(
    repo: str, base_branch: str, branch: str, title: str, body: str
) -> dict:
    resp = requests.post(
        f"{GITHUB_API}/repos/{repo}/pulls",
        headers=github_headers(),
        json={"title": title, "head": branch, "base": base_branch, "body": body},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def ensure_label(repo: str, issue_number: int, label: str) -> None:
    resp = requests.post(
        f"{GITHUB_API}/repos/{repo}/issues/{issue_number}/labels",
        headers=github_headers(),
        json={"labels": [label]},
        timeout=15,
    )
    if resp.status_code != 422:
        resp.raise_for_status()
        return

    requests.post(
        f"{GITHUB_API}/repos/{repo}/labels",
        headers=github_headers(),
        json={"name": label, "color": "0e8a16"},
        timeout=15,
    )
    retry = requests.post(
        f"{GITHUB_API}/repos/{repo}/issues/{issue_number}/labels",
        headers=github_headers(),
        json={"labels": [label]},
        timeout=15,
    )
    retry.raise_for_status()


def build_branch_name(dag_id: str, run_id: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", run_id).strip("-")[:24]
    return f"auto-fix/{dag_id}/{slug}"


def render_pr_body(diagnosis: dict, run_id: str, dag_id: str, alert_id: str) -> str:
    evidence = "\n".join(f"- {line}" for line in (diagnosis.get("evidence") or [])[:10])
    return (
        "**Auto-generated based on Otto's diagnosis.**\n\n"
        f"## Summary\n{diagnosis.get('summary', '')}\n\n"
        "## Root cause\n"
        f"- **Cause:** {diagnosis.get('root_cause', '')}\n"
        f"- **Task:** `{diagnosis.get('root_cause_task', '')}`\n"
        f"- **Type:** `{diagnosis.get('root_cause_type', '')}` ({diagnosis.get('transience', '')})\n\n"
        "## Severity & confidence\n"
        f"- Severity: {diagnosis.get('severity', '')} (priority {diagnosis.get('priority', '')})\n"
        f"- Confidence: {diagnosis.get('confidence', 0):.2f} ({diagnosis.get('confidence_justification', '')})\n\n"
        "## Suggested fix (raw from agent)\n"
        f"{diagnosis.get('suggested_fix', '')}\n\n"
        f"## Evidence\n{evidence}\n\n"
        "---\n"
        f"Generated from failed run `{run_id}` in `{dag_id}`.\n"
        f"Alert ID: `{alert_id}`."
    )


def airflow_base() -> str:
    return os.environ["AIRFLOW_BASE_URL"].rstrip("/")


def airflow_token() -> str:
    env_token = os.environ.get("ASTRO_API_TOKEN")
    if env_token:
        return env_token

    resp = requests.post(
        f"{airflow_base()}/auth/token",
        json={
            "username": os.environ.get("AIRFLOW_USERNAME", "admin"),
            "password": os.environ.get("AIRFLOW_PASSWORD", "admin"),
        },
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    token = payload.get("access_token") or payload.get("jwt")
    if not token:
        raise RuntimeError(f"No token in /auth/token response: {payload}")
    return token


def airflow_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {airflow_token()}",
        "Content-Type": "application/json",
    }


def get_dag_relative_fileloc(dag_id: str) -> str:
    resp = requests.get(
        f"{airflow_base()}/api/v2/dags/{dag_id}",
        headers=airflow_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    fileloc = resp.json().get("relative_fileloc")
    if not fileloc:
        raise RuntimeError(f"No relative_fileloc for {dag_id}")
    return fileloc


def iter_sse_events(response: requests.Response) -> Iterator[tuple[str, str]]:
    event_type: str | None = None
    data_lines: list[str] = []
    for raw in response.iter_lines(decode_unicode=True):
        line = "" if raw is None else raw.rstrip("\r")
        if not line:
            if event_type is not None:
                yield event_type, "\n".join(data_lines)
            event_type, data_lines = None, []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value.lstrip()
        if field == "event":
            event_type = value
        elif field == "data":
            data_lines.append(value)
    if event_type is not None:
        yield event_type, "\n".join(data_lines)


def request_diagnosis(
    org_id: str, deployment_id: str, dag_id: str, run_id: str, token: str
) -> dict:
    base = (
        f"{ASTRO_API_BASE}/organizations/{org_id}"
        f"/observability/deployments/{deployment_id}/dag-failure-diagnosis/runs"
    )
    auth = {"Authorization": f"Bearer {token}"}

    start = requests.post(
        base,
        headers={**auth, "Content-Type": "application/json"},
        json={"dagId": dag_id, "runId": run_id},
        timeout=30,
    )
    start.raise_for_status()
    diagnosis_run_id = start.json()["runId"]

    text_chunks: list[str] = []
    with requests.get(
        f"{base}/{diagnosis_run_id}/events",
        headers={**auth, "Accept": "text/event-stream"},
        stream=True,
        timeout=(30, 600),
    ) as resp:
        resp.raise_for_status()
        for event, data in iter_sse_events(resp):
            if not data:
                continue
            if event == "rca_diagnosis":
                return json.loads(data)
            if event == "error":
                raise RuntimeError(
                    json.loads(data).get(
                        "message", "Investigation Agent returned an error"
                    )
                )
            if event == "text_delta":
                text_chunks.append(json.loads(data).get("text", ""))

    if text_chunks:
        return {
            "title": f"Investigation for {dag_id}",
            "summary": "".join(text_chunks).strip(),
        }
    raise RuntimeError(
        f"Investigation Agent stream ended without a diagnosis for {dag_id} run {run_id}"
    )


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
