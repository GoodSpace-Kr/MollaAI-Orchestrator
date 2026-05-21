from __future__ import annotations

import asyncio
import os
import types
import unittest
from unittest.mock import Mock, patch

from storage import S3AudioStorage


class S3AudioStorageTests(unittest.TestCase):
    def test_build_key_uses_prefix_call_id_and_turn_index(self) -> None:
        storage = S3AudioStorage(
            bucket="molla-call-audio-prod",
            region="ap-northeast-2",
            prefix="calls",
            client=Mock(),
        )

        self.assertEqual(
            storage.build_key(call_id="CA123", turn_index=5),
            "calls/CA123/turns/5.wav",
        )

    def test_upload_turn_audio_returns_generated_key(self) -> None:
        client = Mock()
        storage = S3AudioStorage(
            bucket="molla-call-audio-prod",
            region="ap-northeast-2",
            prefix="calls",
            client=client,
        )

        key = asyncio.run(storage.upload_turn_audio(call_id="CA123", turn_index=5, wav_bytes=b"RIFF...."))

        self.assertEqual(key, "calls/CA123/turns/5.wav")
        client.put_object.assert_called_once_with(
            Bucket="molla-call-audio-prod",
            Key="calls/CA123/turns/5.wav",
            Body=b"RIFF....",
            ContentType="audio/wav",
        )

    def test_client_uses_s3_specific_credentials(self) -> None:
        storage = S3AudioStorage(
            bucket="molla-call-audio-prod",
            region="ap-northeast-2",
            prefix="calls",
        )

        with patch.dict(
            os.environ,
            {
                "AWS_S3_ACCESS_KEY": "s3-access-key",
                "AWS_S3_SECRET_KEY": "s3-secret-key",
            },
            clear=False,
        ):
            fake_boto3 = types.SimpleNamespace(client=Mock())
            with patch.dict("sys.modules", {"boto3": fake_boto3}):
                client = storage._get_client()

        self.assertIs(client, fake_boto3.client.return_value)
        fake_boto3.client.assert_called_once_with(
            "s3",
            region_name="ap-northeast-2",
            aws_access_key_id="s3-access-key",
            aws_secret_access_key="s3-secret-key",
        )


if __name__ == "__main__":
    unittest.main()
