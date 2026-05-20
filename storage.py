from __future__ import annotations

import asyncio
from typing import Any


class S3AudioStorage:
    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        prefix: str = "calls",
        client: Any | None = None,
    ) -> None:
        self.bucket = bucket
        self.region = region
        self.prefix = prefix.strip("/")
        self._client = client

    def build_key(self, *, call_id: str, turn_index: int) -> str:
        normalized_call_id = call_id.strip() or "call-session"
        base_prefix = f"{self.prefix}/" if self.prefix else ""
        return f"{base_prefix}{normalized_call_id}/turns/{turn_index}.wav"

    async def upload_turn_audio(self, *, call_id: str, turn_index: int, wav_bytes: bytes) -> str:
        key = self.build_key(call_id=call_id, turn_index=turn_index)
        await asyncio.to_thread(
            self._get_client().put_object,
            Bucket=self.bucket,
            Key=key,
            Body=wav_bytes,
            ContentType="audio/wav",
        )
        return key

    def _get_client(self) -> Any:
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", region_name=self.region)
        return self._client
