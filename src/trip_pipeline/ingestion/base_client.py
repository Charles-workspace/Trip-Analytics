from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from trip_pipeline.utils.logger import get_logger
from trip_pipeline.utils.retry import retry_on_failure

logger = get_logger(__name__)


class BaseAPIClient:
    """
    Generic HTTP client with:
    - base URL handling
    - optional API key header
    - retry + logging
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        default_headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.default_headers = default_headers or {}
        self.timeout = timeout

    def _build_headers(
        self,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        headers = {**self.default_headers}
        if extra_headers:
            headers.update(extra_headers)

        # NOAA uses "token" header for the API key.
        if self.api_key:
            headers.setdefault("token", self.api_key)

        return headers

    @retry_on_failure(max_retries=3, backoff_seconds=2)
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = self._build_headers()

        logger.info("GET %s params=%s", url, params)
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=self.timeout,
        )
        logger.info("Response %s %s", response.status_code, response.reason)
        response.raise_for_status()
        return response.json()