import json
import os
import re

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from include.airflow_rca import request_diagnosis
from include.fix_edits import FixProposal, apply_edits, ruff_format
from include.github_pr import (
    build_branch_name,
    commit_file,
    create_branch,
    create_pull_request,
    ensure_label,
    find_dag_file_path,
    find_open_pr,
    get_repo_file,
    render_pr_body,
)


@dag
def otto_rca_to_gh_pr():
    @task
    def parse_alert(**context) -> dict:
        dag_run = context.get("dag_run")
        conf = dict(dag_run.conf or {}) if dag_run else {}

        message = str(conf.get("message", ""))
        match = re.search(r"for DAG\s+['\"]?([a-zA-Z0-9_.-]+)", message, re.IGNORECASE)
        if not match:
            raise ValueError(
                f"Could not extract source dag_id from message: {message!r}"
            )

        run_id = conf.get("airflowDagRunId")
        if not run_id:
            raise ValueError("Missing required dag_run.conf key: airflowDagRunId")

        return {
            "source_dag_id": match.group(1),
            "source_run_id": run_id,
            "alert_id": conf.get("alertId", "unknown"),
        }

    @task
    def dedup_check(parsed: dict) -> dict:
        repo = os.environ["GITHUB_REPO"]
        label = f"auto-fix:{parsed['source_dag_id']}"
        existing = find_open_pr(repo, label)
        if existing:
            raise AirflowSkipException(
                f"Open auto-fix PR already exists: #{existing['number']} ({existing['html_url']})"
            )
        return parsed

    @task
    def get_diagnosis(parsed: dict) -> dict:
        return request_diagnosis(
            org_id=os.environ["ASTRO_ORGANIZATION_ID"],
            deployment_id=os.environ["ASTRO_DEPLOYMENT_ID"],
            dag_id=parsed["source_dag_id"],
            run_id=parsed["source_run_id"],
            token=os.environ["ASTRO_API_TOKEN"],
        )

    @task
    def fetch_source_file(parsed: dict) -> dict:
        repo = os.environ["GITHUB_REPO"]
        base_branch = os.environ.get("GITHUB_BASE_BRANCH", "main")
        path = find_dag_file_path(repo, base_branch, parsed["source_dag_id"])
        file = get_repo_file(repo, base_branch, path)
        return {"path": path, "content": file["content"], "sha": file["sha"]}

    @task.agent(
        llm_conn_id="pydanticai_default",
        output_type=FixProposal,
        system_prompt=(
            "You are a principal data engineer. You receive a structured failure "
            "diagnosis and the current source of one Python file. Propose the "
            "smallest set of exact search-and-replace edits that resolve the "
            "diagnosed root cause and nothing else. For each edit, 'old' must be "
            "copied verbatim from the provided file content and must be unique "
            "within the file (include enough surrounding context to be "
            "unambiguous); 'new' is its replacement. Do not reformat or change "
            "unrelated code."
        ),
    )
    def propose_fix(diagnosis: dict, source: dict) -> str:
        return (
            f"=== DIAGNOSIS (JSON) ===\n{json.dumps(diagnosis, indent=2)}\n\n"
            f"=== FILE PATH ===\n{source['path']}\n\n"
            f"=== CURRENT FILE CONTENT ===\n{source['content']}"
        )

    @task
    def open_pr(parsed: dict, diagnosis: dict, source: dict, proposal: FixProposal) -> str:
        repo = os.environ["GITHUB_REPO"]
        base_branch = os.environ.get("GITHUB_BASE_BRANCH", "main")

        patched = apply_edits(source["content"], proposal.model_dump()["edits"])
        formatted = ruff_format(patched)
        branch = build_branch_name(parsed["source_dag_id"], parsed["source_run_id"])

        create_branch(repo, base_branch, branch)
        commit_file(
            repo,
            branch,
            source["path"],
            formatted,
            source["sha"],
            f"[AUTOMATED PR] {diagnosis.get('title', 'Investigation Agent fix')}",
        )

        body = render_pr_body(
            diagnosis,
            parsed["source_run_id"],
            parsed["source_dag_id"],
            parsed["alert_id"],
        )
        pr = create_pull_request(
            repo,
            base_branch,
            branch,
            f"[AUTOMATED PR] {diagnosis.get('title', parsed['source_dag_id'])}",
            body,
        )
        ensure_label(repo, pr["number"], f"auto-fix:{parsed['source_dag_id']}")
        return pr["html_url"]

    parsed = parse_alert()
    parsed = dedup_check(parsed)
    diagnosis = get_diagnosis(parsed)
    source = fetch_source_file(parsed)
    proposal = propose_fix(diagnosis, source)
    open_pr(parsed, diagnosis, source, proposal)


otto_rca_to_gh_pr()
