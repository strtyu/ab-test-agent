from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from ab_agent.core.exceptions import SlackError


class SlackClient:
    def __init__(self) -> None:
        token = os.environ.get("SLACK_BOT_TOKEN", "")
        if not token:
            raise SlackError("SLACK_BOT_TOKEN is not set")
        self._client = WebClient(token=token)

    def send_message(
        self,
        channel: str,
        text: str,
        blocks: Optional[list] = None,
    ) -> str:
        try:
            resp = self._client.chat_postMessage(
                channel=channel,
                text=text,
                blocks=blocks or [],
            )
            return resp["ts"]
        except SlackApiError as e:
            raise SlackError(f"Slack API error: {e.response['error']}") from e

    def upload_file(
        self,
        channel: str,
        file_path: Path,
        title: str,
        initial_comment: str = "",
    ) -> None:
        try:
            self._client.files_upload_v2(
                channel=channel,
                file=str(file_path),
                filename=file_path.name,
                title=title,
                initial_comment=initial_comment,
            )
        except SlackApiError as e:
            raise SlackError(f"Slack file upload error: {e.response['error']}") from e

    def send_analysis_report(
        self,
        channel: str,
        blocks: List[dict],
        screenshot_path: Optional[Path] = None,
        comment: str = "",
    ) -> None:
        self.send_message(channel=channel, text=comment or "A/B Test Results", blocks=blocks)
        if screenshot_path and screenshot_path.exists():
            self.upload_file(
                channel=channel,
                file_path=screenshot_path,
                title="A/B Test Dashboard",
                initial_comment="",
            )
