"""Raw 80-byte Bitcoin block header (de)serialization and hash validation.

Used by the stale-blocks pipeline (btc_parser_app.rpc.stale_blocks) to parse
headers pulled from the GitHub stale-blocks dataset or `getblockheader ...
false`, and to verify a claimed blockhash actually matches its header before
trusting it - a header that doesn't hash to the claimed value is exactly as
fake as a bare hash with no header at all, just harder to notice.
"""

from __future__ import annotations

import hashlib
import struct

HEADER_LENGTH_BYTES = 80


class HeaderParseError(ValueError):
    """Raised for a header hex string that isn't exactly 80 bytes."""


def _double_sha256(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def header_hash(header_hex: str) -> str:
    """The blockhash a raw header serializes to: double-SHA256 of the 80 raw
    bytes, byte-reversed to Bitcoin's display/internal byte order."""
    raw = bytes.fromhex(header_hex)
    if len(raw) != HEADER_LENGTH_BYTES:
        raise HeaderParseError(
            f"expected {HEADER_LENGTH_BYTES}-byte header, got {len(raw)} bytes"
        )
    return _double_sha256(raw)[::-1].hex()


def validate_header_hash(header_hex: str | None, claimed_hash: str) -> bool:
    """False for anything that isn't a well-formed 80-byte header hashing to
    claimed_hash - including a missing/malformed header_hex, so callers can
    pass it directly without a None-check first."""
    if not header_hex:
        return False
    try:
        return header_hash(header_hex) == claimed_hash.lower()
    except (HeaderParseError, ValueError):
        return False


def parse_header(header_hex: str) -> dict[str, object]:
    """Decode the 80 raw header bytes into the same field names
    `getblockheader` returns: previousblockhash/merkleroot byte-reversed to
    display order, bits as an 8-hex-digit string, time/nonce/version as
    plain integers."""
    raw = bytes.fromhex(header_hex)
    if len(raw) != HEADER_LENGTH_BYTES:
        raise HeaderParseError(
            f"expected {HEADER_LENGTH_BYTES}-byte header, got {len(raw)} bytes"
        )

    (version,) = struct.unpack_from("<i", raw, 0)
    previousblockhash = raw[4:36][::-1].hex()
    merkleroot = raw[36:68][::-1].hex()
    (block_time,) = struct.unpack_from("<I", raw, 68)
    (bits,) = struct.unpack_from("<I", raw, 72)
    (nonce,) = struct.unpack_from("<I", raw, 76)

    return {
        "version": version,
        "previousblockhash": previousblockhash,
        "merkleroot": merkleroot,
        "time": block_time,
        "bits": f"{bits:08x}",
        "nonce": nonce,
    }
