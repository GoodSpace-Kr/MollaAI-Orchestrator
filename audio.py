from __future__ import annotations

import base64
from dataclasses import dataclass, field

import numpy as np


MU_LAW_BIAS = 0x84
MU_LAW_CLIP = 32635
WAV_HEADER_BYTES = 44

def decode_base64_payload(payload: str) -> bytes:
    return base64.b64decode(payload.encode("ascii"))


def encode_base64_payload(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def mulaw_to_pcm16(payload: bytes) -> np.ndarray:
    if not payload:
        return np.array([], dtype=np.int16)

    encoded = np.frombuffer(payload, dtype=np.uint8).astype(np.int16)
    encoded = np.bitwise_xor(encoded, 0xFF)

    sign = encoded & 0x80
    exponent = (encoded >> 4) & 0x07
    mantissa = encoded & 0x0F

    magnitude = ((mantissa << 3) + MU_LAW_BIAS) << exponent
    decoded = magnitude - MU_LAW_BIAS
    decoded = np.where(sign != 0, -decoded, decoded)
    return decoded.astype(np.int16, copy=False)


def pcm16_to_mulaw(samples: np.ndarray) -> bytes:
    if samples.size == 0:
        return b""

    pcm = np.asarray(samples, dtype=np.int16).astype(np.int32)
    sign = np.where(pcm < 0, 0x80, 0x00).astype(np.int32)
    magnitude = np.minimum(np.abs(pcm), MU_LAW_CLIP) + MU_LAW_BIAS

    safe_magnitude = np.maximum(magnitude, 1)
    exponent = np.clip(np.floor(np.log2(safe_magnitude)).astype(np.int32) - 7, 0, 7)
    mantissa = (safe_magnitude >> (exponent + 3)) & 0x0F
    encoded = np.bitwise_xor(sign | (exponent << 4) | mantissa, 0xFF)
    return encoded.astype(np.uint8).tobytes()


def pcm16_to_bytes(samples: np.ndarray) -> bytes:
    pcm = np.asarray(samples, dtype=np.int16)
    return pcm.astype("<i2", copy=False).tobytes()


def bytes_to_pcm16(payload: bytes) -> np.ndarray:
    if not payload:
        return np.array([], dtype=np.int16)
    return np.frombuffer(payload, dtype="<i2").astype(np.int16, copy=False)


def resample_pcm16(samples: np.ndarray, source_rate: int, target_rate: int) -> np.ndarray:
    audio = np.asarray(samples, dtype=np.int16)
    if audio.size == 0 or source_rate == target_rate:
        return audio

    source = audio.astype(np.float32)
    target_length = max(1, int(round(source.size * target_rate / source_rate)))
    source_positions = np.arange(source.size, dtype=np.float32)
    target_positions = np.linspace(0, max(source.size - 1, 0), num=target_length, dtype=np.float32)
    resampled = np.interp(target_positions, source_positions, source)
    return np.clip(np.round(resampled), -32768, 32767).astype(np.int16)


@dataclass(slots=True)
class WavStreamBuffer:
    header_bytes_expected: int = WAV_HEADER_BYTES
    _buffer: bytearray = field(default_factory=bytearray, init=False)
    _header_consumed: bool = False

    def push(self, chunk: bytes) -> bytes:
        if not chunk:
            return b""

        self._buffer.extend(chunk)
        if not self._header_consumed:
            if len(self._buffer) < self.header_bytes_expected:
                return b""
            del self._buffer[: self.header_bytes_expected]
            self._header_consumed = True

        pcm_length = len(self._buffer) - (len(self._buffer) % 2)
        if pcm_length <= 0:
            return b""

        pcm = bytes(self._buffer[:pcm_length])
        del self._buffer[:pcm_length]
        return pcm
