from __future__ import annotations

from typing import Any, Dict, Iterator, Optional
from urllib.parse import urljoin

import requests


class AAPClientError(RuntimeError):
    pass


class AAPClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        api_path: str = "/api/v2",
        verify_ssl: bool = True,
        timeout: int = 30,
        page_size: int = 200,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.api_path = "/" + api_path.strip("/")
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.page_size = page_size
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
        )

    def close(self) -> None:
        self.session.close()

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            return path_or_url
        if path_or_url.startswith("/api/"):
            return urljoin(self.base_url, path_or_url.lstrip("/"))
        return urljoin(
            self.base_url,
            f"{self.api_path.strip('/')}/{path_or_url.lstrip('/')}"
        )

    def get(self, path_or_url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            response = self.session.get(
                self._url(path_or_url),
                params=params,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise AAPClientError(f"AAP API request failed for {path_or_url}: {exc}") from exc
        if not isinstance(payload, dict):
            raise AAPClientError(f"Unexpected AAP API response for {path_or_url}")
        return payload

    def iter_results(
        self,
        path_or_url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        query = dict(params or {})
        query.setdefault("page_size", self.page_size)
        next_url: Optional[str] = path_or_url
        first = True
        while next_url:
            payload = self.get(next_url, params=query if first else None)
            for item in payload.get("results", []):
                if isinstance(item, dict):
                    yield item
            next_url = payload.get("next")
            first = False

    def workflow_runs(
        self,
        template_id: int,
        started_gte: str,
        started_lt: str,
        after_id: int = 0,
    ) -> Iterator[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "workflow_job_template": template_id,
            "started__gte": started_gte,
            "started__lt": started_lt,
            "order_by": "id",
        }
        if after_id:
            params["id__gt"] = after_id
        yield from self.iter_results("workflow_jobs/", params=params)

    def workflow_nodes(self, workflow_job_id: int) -> Iterator[Dict[str, Any]]:
        yield from self.iter_results(f"workflow_jobs/{workflow_job_id}/workflow_nodes/")

    def related(self, obj: Dict[str, Any], relation: str) -> Optional[Dict[str, Any]]:
        related_url = obj.get("related", {}).get(relation)
        if not related_url:
            return None
        return self.get(related_url)

    def related_results(self, obj: Dict[str, Any], relation: str) -> Iterator[Dict[str, Any]]:
        related_url = obj.get("related", {}).get(relation)
        if not related_url:
            return
        yield from self.iter_results(related_url)

    def inventory(self, inventory_id: int) -> Dict[str, Any]:
        return self.get(f"inventories/{inventory_id}/")
