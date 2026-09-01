from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from .aap_client import AAPClient
from .postgres_store import PostgresStore


class WorkflowCollector:
    def __init__(self, client: AAPClient, store: PostgresStore) -> None:
        self.client = client
        self.store = store
        self._visited_workflows: Set[int] = set()
        self._visited_jobs: Set[int] = set()
        self._inventories: Set[int] = set()

    @staticmethod
    def empty_metrics() -> Dict[str, int]:
        return {
            "workflows": 0,
            "jobs": 0,
            "successful_jobs": 0,
            "failed_jobs": 0,
            "hosts": 0,
            "events": 0,
            "changed": 0,
            "failed_events": 0,
            "unreachable": 0,
        }

    def collect_root(self, root: Dict[str, Any]) -> Dict[str, int]:
        root_id = int(root["id"])
        self._visited_workflows.clear()
        self._visited_jobs.clear()
        self._inventories.clear()
        metrics = self.empty_metrics()
        self._collect_workflow(
            workflow=root,
            root_id=root_id,
            parent_workflow_id=None,
            depth=0,
            metrics=metrics,
        )
        self.store.upsert_metrics(root_id, metrics)
        return metrics

    def _collect_workflow(
        self,
        workflow: Dict[str, Any],
        root_id: int,
        parent_workflow_id: Optional[int],
        depth: int,
        metrics: Dict[str, int],
    ) -> None:
        workflow_id = int(workflow["id"])
        if workflow_id in self._visited_workflows:
            return
        self._visited_workflows.add(workflow_id)
        metrics["workflows"] += 1
        self.store.upsert_workflow(root_id, parent_workflow_id, depth, workflow)

        for node in self.client.workflow_nodes(workflow_id):
            unified = self.client.related(node, "unified_job")
            self.store.upsert_node(root_id, workflow_id, depth + 1, node, unified)
            if not unified:
                continue

            unified_type = unified.get("type")
            if unified_type == "workflow_job":
                self._collect_workflow(
                    workflow=unified,
                    root_id=root_id,
                    parent_workflow_id=workflow_id,
                    depth=depth + 1,
                    metrics=metrics,
                )
            elif unified_type == "job":
                self._collect_job(
                    job=unified,
                    root_id=root_id,
                    parent_workflow_id=workflow_id,
                    node_id=int(node["id"]),
                    depth=depth + 1,
                    metrics=metrics,
                )

    def _collect_job(
        self,
        job: Dict[str, Any],
        root_id: int,
        parent_workflow_id: int,
        node_id: int,
        depth: int,
        metrics: Dict[str, int],
    ) -> None:
        job_id = int(job["id"])
        if job_id in self._visited_jobs:
            return
        self._visited_jobs.add(job_id)

        self.store.upsert_job(root_id, parent_workflow_id, node_id, depth, job)
        metrics["jobs"] += 1
        if job.get("status") == "successful":
            metrics["successful_jobs"] += 1
        elif job.get("status") in {"failed", "error", "canceled"}:
            metrics["failed_jobs"] += 1

        inventory_id = job.get("inventory")
        if inventory_id and int(inventory_id) not in self._inventories:
            inventory = self.client.inventory(int(inventory_id))
            self.store.upsert_inventory(inventory)
            self._inventories.add(int(inventory_id))

        host_rows = list(self.client.related_results(job, "job_host_summaries"))
        metrics["hosts"] += self.store.upsert_host_summaries(root_id, job_id, host_rows)

        event_metrics = self.store.upsert_events(
            root_id,
            job_id,
            self.client.related_results(job, "job_events"),
        )
        metrics["events"] += event_metrics["events"]
        metrics["changed"] += event_metrics["changed"]
        metrics["failed_events"] += event_metrics["failed"]
        metrics["unreachable"] += event_metrics["unreachable"]


def plan_roots(client: AAPClient, template_id: int, after_id: int = 0, limit: int = 0) -> List[Dict[str, Any]]:
    roots: List[Dict[str, Any]] = []
    for root in client.workflow_runs(template_id, after_id=after_id):
        roots.append(root)
        if limit and len(roots) >= limit:
            break
    return roots
