import json
from collections.abc import Iterator

import requests

ASTRO_API_BASE = "https://api.astronomer.io/labs/v1"


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
