# SPDX-FileCopyrightText: Groundbox
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Whole files over Groundbox's file API, which moves at most one bounded request a call.

Groundbox's `PUT /files` takes a body of up to 32 MiB through its front end, and `GET /files`
serves a file of up to 100 MB. A file within those bounds is one call. A larger one moves in parts:

- a write uploads parts beside the destination, then one command in the sandbox appends them to
  a temporary file and renames it over the destination, so the destination is the whole file or
  is left as it was;
- a read copies one slice of the file at a time to a temporary file with `dd` and downloads it.

The commands run through `sudo -n`, as Groundbox's file API itself is not bound by the default
user's permissions. Groundbox's own image has `sudo`, `dd`, `cat` and `stat`.
"""

from __future__ import annotations

import asyncio
import math
import secrets
from collections.abc import AsyncIterable, AsyncIterator
from typing import TYPE_CHECKING, Any

import httpx
from sandbox_provider_spec.v1 import FileInfo, FileStream, SpsError

from sps_provider_groundbox.facts import FILE_BYTES

if TYPE_CHECKING:
    from sps_provider_groundbox import GroundboxProvider

WRITE_PART = 16 * 1024 * 1024  # under the 32 MiB a request body may have
WRITE_PARALLEL = 4  # parts in flight, each held in memory
READ_PART_MIB = 64  # under the 100 MB a download may have
READ_PART = READ_PART_MIB * 1024 * 1024
_NO_TIMEOUT = httpx.Timeout(None, connect=10.0)
_SLOW = httpx.Timeout(900.0, connect=10.0)

# $1 the temporary file, $2 the destination, then the parts in order.
_JOIN = """set -e
t="$1"; d="$2"; shift 2
: > "$t"
chown --reference="$1" "$t"
for p in "$@"; do cat "$p" >> "$t"; rm -f "$p"; done
mv -f "$t" "$d"
"""


async def _exec(provider: GroundboxProvider, sandbox_id: str, args: list[str]) -> dict[str, Any]:
    body = {"args": args, "timeout_s": 900}
    return await provider._json(  # pylint: disable=protected-access
        "POST", f"/sandboxes/{sandbox_id}/exec", json=body, timeout=_SLOW
    )


async def _remove(provider: GroundboxProvider, sandbox_id: str, paths: list[str]) -> None:
    """Remove temporary files. Best effort: a failure leaves them and changes no result."""
    try:
        await _exec(provider, sandbox_id, ["sudo", "-n", "rm", "-f", "--", *paths])
    except SpsError:
        pass


async def _blocks(data: AsyncIterable[bytes], size: int) -> AsyncIterator[bytes]:
    """The stream again, in blocks of exactly `size` bytes (the last one may be shorter)."""
    held = bytearray()
    async for chunk in data:
        held += chunk
        while len(held) >= size:
            yield bytes(held[:size])
            del held[:size]
    if held:
        yield bytes(held)


def _wrong_size(size: int) -> SpsError:
    return SpsError(
        "invalid_request", f"The body is not the {size} bytes that were declared", outcome="unknown"
    )


async def write(
    provider: GroundboxProvider, sandbox_id: str, path: str, data: AsyncIterable[bytes], size: int
) -> FileInfo:
    if size < 0:
        raise SpsError("invalid_request", "size cannot be negative")
    if size > FILE_BYTES:
        raise SpsError("too_large", f"A file is at most {FILE_BYTES} bytes")
    files = f"/sandboxes/{sandbox_id}/files"
    if size <= WRITE_PART:
        whole = bytearray()
        async for chunk in data:
            whole += chunk
            if len(whole) > size:
                raise _wrong_size(size)
        if len(whole) != size:
            raise _wrong_size(size)
        await provider._req(  # pylint: disable=protected-access
            "PUT", files, params={"path": path}, content=bytes(whole), timeout=_SLOW
        )
        return FileInfo(path=path, size=size)

    stem = f"{path}.sps-part-{secrets.token_hex(6)}"
    names: list[str] = []
    pending: set[asyncio.Task[None]] = set()

    async def put(name: str, block: bytes) -> None:
        await provider._req(  # pylint: disable=protected-access
            "PUT", files, params={"path": name}, content=block, timeout=_SLOW
        )

    try:
        total = 0
        async for block in _blocks(data, WRITE_PART):
            total += len(block)
            if total > size:
                raise _wrong_size(size)
            names.append(f"{stem}-{len(names):05d}")
            pending.add(asyncio.ensure_future(put(names[-1], block)))
            if len(pending) >= WRITE_PARALLEL:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
        if pending:
            done, pending = await asyncio.wait(pending)
            for task in done:
                task.result()
        if total != size:
            raise _wrong_size(size)
        joined = await _exec(
            provider,
            sandbox_id,
            ["sudo", "-n", "sh", "-c", _JOIN, "sh", f"{stem}-tmp", path, *names],
        )
        if joined["exit_code"] != 0 or joined["timed_out"]:
            raise SpsError(
                "provider_error",
                "Groundbox could not put the file's parts together",
                outcome="unknown",
            )
    except BaseException:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await _remove(provider, sandbox_id, [f"{stem}-tmp", *names])
        raise
    return FileInfo(path=path, size=size)


async def _download(
    provider: GroundboxProvider, sandbox_id: str, path: str
) -> tuple[httpx.Response | None, SpsError | None]:
    """Open `GET /files?path=`: the open response, or the error it answered with."""
    request = provider._http.build_request(  # pylint: disable=protected-access
        "GET", f"/sandboxes/{sandbox_id}/files", params={"path": path}, timeout=_NO_TIMEOUT
    )
    try:
        resp = await provider._http.send(request, stream=True)  # pylint: disable=protected-access
    except httpx.TimeoutException:
        raise SpsError("timeout", "Groundbox did not answer", outcome="unknown") from None
    except httpx.HTTPError:
        raise SpsError("unavailable", "Groundbox is unreachable", outcome="unknown") from None
    if resp.status_code < 400:
        return resp, None
    try:
        await resp.aread()
    except httpx.HTTPError:
        pass
    finally:
        await resp.aclose()
    return None, provider._error(resp)  # pylint: disable=protected-access


async def _body(resp: httpx.Response) -> AsyncIterator[bytes]:
    """The response's bytes. A connection that ends early is an error, never a short file."""
    try:
        async for chunk in resp.aiter_bytes(1 << 16):
            yield chunk
    except httpx.HTTPError:
        raise SpsError(
            "provider_error", "The download ended before the whole file", outcome="unknown"
        ) from None
    finally:
        await resp.aclose()


async def _sliced(
    provider: GroundboxProvider, sandbox_id: str, path: str, size: int
) -> AsyncIterator[bytes]:
    part = f"/tmp/.sps-read-{secrets.token_hex(6)}"
    sent = 0
    try:
        for index in range(math.ceil(size / READ_PART)):
            copied = await _exec(
                provider,
                sandbox_id,
                [
                    "sudo", "-n", "dd", f"if={path}", f"of={part}", "bs=1M",
                    f"skip={index * READ_PART_MIB}", f"count={READ_PART_MIB}",
                    "iflag=fullblock", "status=none",
                ],
            )  # fmt: skip
            if copied["exit_code"] != 0 or copied["timed_out"]:
                raise SpsError(
                    "provider_error", "Groundbox could not read the file", outcome="unknown"
                )
            resp, error = await _download(provider, sandbox_id, part)
            if resp is None:
                assert error is not None
                raise SpsError(
                    "provider_error", "Groundbox could not read the file", outcome="unknown"
                )
            async for chunk in _body(resp):
                sent += len(chunk)
                yield chunk
        if sent != size:
            raise SpsError(
                "provider_error", "The file changed while it was read", outcome="unknown"
            )
    finally:
        await _remove(provider, sandbox_id, [part])


async def read(provider: GroundboxProvider, sandbox_id: str, path: str) -> FileStream:
    resp, error = await _download(provider, sandbox_id, path)
    if resp is not None:
        return FileStream(size=None, chunks=_body(resp))
    assert error is not None
    if error.code != "invalid_request":
        raise error
    # Groundbox refuses a directory and a file over its download limit alike. Ask the sandbox
    # what the path is (following a link, as the download does).
    found = await _exec(
        provider, sandbox_id, ["sudo", "-n", "stat", "-L", "-c", "%s %F", "--", path]
    )
    if found["exit_code"] != 0:
        raise error
    text = str(found["stdout"]).strip()
    length, _, kind = text.partition(" ")
    if not kind.startswith("regular") or not length.isdigit():
        raise SpsError("invalid_request", f"{path} is not a file"[:1024])
    size = int(length)
    if size > FILE_BYTES:
        raise SpsError("too_large", f"A file is at most {FILE_BYTES} bytes")
    if size <= READ_PART:
        raise error
    return FileStream(size=size, chunks=_sliced(provider, sandbox_id, path, size))
