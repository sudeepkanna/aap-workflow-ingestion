from __future__ import annotations

from typing import Any, Dict, Iterator, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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
        self._base_origin = self._origin(self.base_url)

        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)

        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
        )

    @staticmethod
    def _origin(url: str) -> tuple[str, str]:
        parsed = urlparse(url)
        return parsed.scheme.lower(), parsed.netloc.lower()

    def close(self) -> None:
        self.session.close()

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            if self._origin(path_or_url) != self._base_origin:
                raise AAPClientError("AAP pagination/related URL points outside the configured controller")
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
            results = payload.get("results", [])
            if not isinstance(results, list):
                raise AAPClientError(f"Unexpected paginated response for {next_url}")
            for item in results:
                if isinstance(item, dict):
                    yield item
            next_value = payload.get("next")
            if next_value is not None and not isinstance(next_value, str):
                raise AAPClientError(f"Unexpected pagination link for {next_url}")
            next_url = next_value
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
