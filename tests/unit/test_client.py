import pytest

from plugins.module_utils.aap_client import AAPClient, AAPClientError


class RecordingClient(AAPClient):
    def __init__(self):
        self.calls = []

    def iter_results(self, path_or_url, params=None):
        self.calls.append((path_or_url, params))
        return iter([])


def test_workflow_runs_uses_date_window_and_checkpoint():
    client = RecordingClient()

    list(
        client.workflow_runs(
            template_id=42,
            started_gte="2026-08-31T18:30:00+00:00",
            started_lt="2026-09-01T18:30:00+00:00",
            after_id=500,
        )
    )

    path, params = client.calls[0]
    assert path == "workflow_jobs/"
    assert params == {
        "workflow_job_template": 42,
        "started__gte": "2026-08-31T18:30:00+00:00",
        "started__lt": "2026-09-01T18:30:00+00:00",
        "order_by": "id",
        "id__gt": 500,
    }


def test_absolute_related_url_must_remain_on_controller_origin():
    client = AAPClient("https://aap.example.com", "token")
    try:
        assert client._url("https://aap.example.com/api/v2/jobs/1/") == (
            "https://aap.example.com/api/v2/jobs/1/"
        )
        with pytest.raises(AAPClientError):
            client._url("https://other.example.com/api/v2/jobs/1/")
    finally:
        client.close()
