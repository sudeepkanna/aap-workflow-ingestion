from plugins.module_utils.collector import WorkflowCollector


class FakeClient:
    def __init__(self):
        self.workflows = {
            100: [{"id": 1, "identifier": "DT", "related": {"unified_job": "/u/200"}}],
            200: [
                {"id": 2, "identifier": "domain-a", "related": {"unified_job": "/u/300"}},
                {"id": 3, "identifier": "domain-b", "related": {"unified_job": "/u/301"}},
            ],
        }
        self.unified = {
            "/u/200": {"id": 200, "type": "workflow_job", "name": "DT", "summary_fields": {}},
            "/u/300": {"id": 300, "type": "job", "name": "domain-a", "status": "successful", "summary_fields": {}},
            "/u/301": {"id": 301, "type": "job", "name": "domain-b", "status": "failed", "summary_fields": {}},
        }

    def workflow_nodes(self, workflow_id):
        yield from self.workflows.get(workflow_id, [])

    def related(self, obj, relation):
        return self.unified.get(obj.get("related", {}).get(relation))

    def related_results(self, obj, relation):
        return iter([])

    def inventory(self, inventory_id):
        raise AssertionError("inventory should not be requested")


class FakeStore:
    def __init__(self):
        self.workflow_ids = []
        self.job_ids = []
        self.metrics = None

    def upsert_workflow(self, root_id, parent_id, depth, item):
        self.workflow_ids.append((item["id"], parent_id, depth))

    def upsert_node(self, *args):
        return None

    def upsert_job(self, root_id, parent_id, node_id, depth, job):
        self.job_ids.append((job["id"], parent_id, depth))

    def upsert_host_summaries(self, root_id, job_id, rows):
        return 0

    def upsert_events(self, root_id, job_id, rows):
        return {"events": 0, "changed": 0, "failed": 0, "unreachable": 0}

    def upsert_inventory(self, inventory):
        return None

    def upsert_metrics(self, root_id, metrics):
        self.metrics = dict(metrics)


def test_recursive_workflow_tree_has_no_fixed_fanout_or_depth():
    store = FakeStore()
    collector = WorkflowCollector(FakeClient(), store)
    root = {"id": 100, "type": "workflow_job", "name": "root", "summary_fields": {}}

    metrics = collector.collect_root(root)

    assert store.workflow_ids == [(100, None, 0), (200, 100, 1)]
    assert {row[0] for row in store.job_ids} == {300, 301}
    assert metrics["workflows"] == 2
    assert metrics["jobs"] == 2
    assert metrics["successful_jobs"] == 1
    assert metrics["failed_jobs"] == 1
