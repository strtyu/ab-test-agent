from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from tenacity import retry, stop_after_attempt, wait_exponential

from ab_agent.core.config_loader import get_settings
from ab_agent.core.exceptions import BQDryRunError, BQQueryError


class BQClient:
    def __init__(self) -> None:
        settings = get_settings()
        bq_cfg = settings["bigquery"]
        self.project = bq_cfg.get("project", "")
        credentials_path = bq_cfg.get("credentials_path", "")

        cache_dir = Path(os.environ.get("BQ_CACHE_DIR") or bq_cfg.get("result_cache_dir", "/tmp/.bq_cache"))
        # On Vercel, only /tmp is writable — fallback if configured path fails
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            cache_dir = Path("/tmp/.bq_cache")
            cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache_dir = cache_dir

        # Support inline JSON credentials via GOOGLE_CREDENTIALS_JSON env var (for Vercel)
        credentials_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if credentials_json:
            info = json.loads(credentials_json)
            creds = service_account.Credentials.from_service_account_info(info)
            self._client = bigquery.Client(project=self.project, credentials=creds)
        elif credentials_path and Path(credentials_path).exists():
            creds = service_account.Credentials.from_service_account_file(credentials_path)
            self._client = bigquery.Client(project=self.project, credentials=creds)
        else:
            _creds_path = Path(credentials_path) if credentials_path else Path("sa.json")
            if not _creds_path.exists():
                raise RuntimeError(
                    f"BigQuery service account file not found: '{_creds_path.resolve()}'. "
                    "Please place your GCP service account JSON at that path, or set "
                    "GOOGLE_APPLICATION_CREDENTIALS in your .env file to the correct path. "
                    "Download it from: GCP Console → IAM → Service Accounts → Keys → Add Key."
                )
            self._client = bigquery.Client(project=self.project)

    def _cache_path(self, sql: str) -> Path:
        key = hashlib.sha256(sql.encode()).hexdigest()[:16]
        return self._cache_dir / f"{key}.parquet"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def execute(self, sql: str, use_cache: bool = True) -> pd.DataFrame:
        cache_path = self._cache_path(sql)
        if use_cache and cache_path.exists():
            return pd.read_parquet(cache_path)
        try:
            rows = self._client.query(sql).result()
            df = pd.DataFrame([dict(row) for row in rows])
        except Exception as e:
            raise BQQueryError(f"BigQuery query failed: {e}") from e
        if use_cache:
            df.to_parquet(cache_path, index=False)
        return df

    def dry_run(self, sql: str) -> int:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        try:
            job = self._client.query(sql, job_config=job_config)
            return job.total_bytes_processed
        except Exception as e:
            raise BQDryRunError(str(e), sql=sql) from e
