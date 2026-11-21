# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Prepared images on Blaxel: a build that Blaxel runs as a sandbox, whose record is the image.

Blaxel builds an image when a sandbox is created with `?upload=true` and a zip of a build context
is sent to the presigned URL it answers with. The image (`sandbox/<name>:<tag>`) outlives the
sandbox, but it carries no labels and no request fingerprint, and a deployed sandbox of the
same name is the only thing that Blaxel lets us annotate. So the **build sandbox is kept as the
image's record**:

- the image's id is the sandbox's name, `sps-img-` and 32 hex digits of the `Idempotency-Key`'s
  digest, so a repeated prepare finds the first one;
- the record is labeled `sps-kind=image` and the caller's labels (as `l.<key>`), and its display
  name is the request's fingerprint, so the same key with another request is `conflict`;
- `image.get` reads the record (its status, and its events when the build failed) and the image
  (`GET /images/sandbox/<name>`: its tag and size), so the adapter keeps no state;
- `image.delete` deletes the record and the image. A record that has been deleted (its status or
  its delete event) is gone to every operation here;
- `create` resolves the id to `sandbox/<name>:<tag>` (`resolve`).

`list` of sandboxes skips the records (see `is_record`), and no sandbox operation takes an image's
id. A build that Blaxel has not finished after `BUILD_LIMIT_S` is reported as failed.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import tarfile
import tempfile
import zipfile
from collections.abc import AsyncIterable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import IO, TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

from sandbox_provider_spec.v1 import (
    DockerfileSource,
    ImageDeleteResult,
    ImagePage,
    ImageRequest,
    OciSource,
    PreparedImage,
    PrivateOciSource,
    SpsError,
)

from sps_provider_blaxel import dockerfile, wire

if TYPE_CHECKING:
    from sps_provider_blaxel import BlaxelProvider

PREFIX = "sps-img-"
_ID = re.compile(rf"{PREFIX}[0-9a-f]{{32}}")
KIND = "sps-kind"
USER = "l."
CONTEXT_BYTES = 512 * 1024 * 1024
MAX_FILES = 10_000
MAX_LABELS = 32
BUILD_MEMORY_MIB = 4096  # the build sandbox's own size
BUILD_LIMIT_S = 1800
SETTLE_LIMIT_S = 120
SWEEP_LIMIT = 10
_CHUNK = 1 << 16
_DELETE_EVENT = "ai.blaxel.controlplane.api.delete"
_RESERVED = ("Dockerfile", "blaxel.toml")
_BUILDING = frozenset({"DEPLOYING", "UPDATING", "CREATED", "UPLOADING", "BUILDING"})


def is_record(native: dict[str, Any]) -> bool:
    """Whether a sandbox is an image's record, not a sandbox of the caller's."""
    labels = (native.get("metadata") or {}).get("labels") or {}
    return labels.get(KIND) == "image"


def name_of(key: str) -> str:
    return PREFIX + hashlib.sha256(f"sps-blaxel-image:{key}".encode()).hexdigest()[:32]


def _fingerprint(request: ImageRequest, context: str) -> str:
    raw = json.dumps(asdict(request), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{raw}\0{context}".encode()).hexdigest()[:32]


def _deleted(native: dict[str, Any]) -> bool:
    """Whether the record has been deleted: Blaxel says DELETING, or it logged the delete."""
    return native["status"] == "DELETING" or any(
        isinstance(e, dict) and e.get("type") == _DELETE_EVENT for e in native.get("events") or []
    )


# -- the build context ---------------------------------------------------------------------


async def _spool(
    context: AsyncIterable[bytes] | None, size: int | None
) -> tuple[IO[bytes] | None, str]:
    """The build context on disk, never held whole in memory, and its digest."""
    if context is None:
        return None, ""
    if size is not None and size > CONTEXT_BYTES:
        raise SpsError("too_large", f"A build context is at most {CONTEXT_BYTES} bytes")
    spool = tempfile.TemporaryFile()  # noqa: SIM115
    digest, total = hashlib.sha256(), 0
    try:
        async for chunk in context:
            total += len(chunk)
            if total > CONTEXT_BYTES:
                raise SpsError("too_large", f"A build context is at most {CONTEXT_BYTES} bytes")
            digest.update(chunk)
            spool.write(chunk)
    except BaseException:
        spool.close()
        raise
    spool.seek(0)
    return spool, digest.hexdigest()


def _member(name: str) -> str:
    path = name.removeprefix("./")
    if not path or path.startswith("/") or ".." in path.split("/"):
        raise SpsError(
            "invalid_request", "A context path stays inside the context", field="context"
        )
    return path


def _archive(text: str, spool: IO[bytes] | None) -> IO[bytes]:
    """The zip that Blaxel builds from: the context's files, `Dockerfile` and `blaxel.toml`."""
    out = tempfile.TemporaryFile()  # noqa: SIM115
    try:
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
            if spool is not None:
                try:
                    tar = tarfile.open(fileobj=spool, mode="r:*")  # noqa: SIM115
                except tarfile.TarError:
                    raise SpsError(
                        "invalid_request", "The context is not a tar archive", field="context"
                    ) from None
                with tar:
                    for count, member in enumerate(tar, 1):
                        if count > MAX_FILES:
                            raise SpsError(
                                "too_large",
                                f"A context has at most {MAX_FILES} files",
                                field="context",
                            )
                        path = _member(member.name)
                        if path in _RESERVED:
                            raise SpsError(
                                "unsupported",
                                f"Blaxel reserves {path} in a build context",
                                field="context",
                            )
                        if member.isdir():
                            z.writestr(path.rstrip("/") + "/", b"")
                        elif member.isfile():
                            source = tar.extractfile(member)
                            assert source is not None
                            with source, z.open(zipfile.ZipInfo(path), "w") as sink:
                                while chunk := source.read(_CHUNK):
                                    sink.write(chunk)
                        else:
                            raise SpsError(
                                "unsupported",
                                f"A context holds only files and directories ({path})",
                                field="context",
                            )
            z.writestr("Dockerfile", text)
            z.writestr("blaxel.toml", dockerfile.BLAXEL_TOML)
    except BaseException:
        out.close()
        raise
    out.seek(0)
    return out


# -- reading -------------------------------------------------------------------------------


async def _native(p: BlaxelProvider, name: str) -> dict[str, Any] | None:
    resp = await p.ctl_raw("GET", f"/sandboxes/{quote(name, safe='')}")
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise wire.failure(resp, "the image")
    native: dict[str, Any] = resp.json()
    return native


async def _image(p: BlaxelProvider, name: str) -> dict[str, Any] | None:
    resp = await p.ctl_raw("GET", f"/images/sandbox/{quote(name, safe='')}")
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise wire.failure(resp, "the image")
    found: dict[str, Any] = resp.json()
    return found


async def _record(p: BlaxelProvider, image_id: str) -> dict[str, Any]:
    """The record of an image, or `not_found` for an id that is not one or has been deleted."""
    native = await _native(p, image_id) if _ID.fullmatch(image_id) else None
    if native is None or not is_record(native) or _deleted(native):
        raise SpsError("not_found", "Blaxel holds no such image")
    return native


def _tag(image: dict[str, Any]) -> str | None:
    tags = (image.get("spec") or {}).get("tags") or []
    names = [t for t in sorted(tags, key=lambda t: t.get("createdAt", "")) if t.get("name")]
    return str(names[-1]["name"]) if names else None


def _log(native: dict[str, Any]) -> str:
    """The builder's own log, from the failed build's event. It is read and never echoed."""
    return "\n".join(
        str(e.get("message") or "")
        for e in native.get("events") or []
        if isinstance(e, dict) and e.get("status") == "FAILED"
    )


_STEP = re.compile(r"^#(\d+) \[[^\]]*? (\d+)/\d+\] (\w+)", re.MULTILINE)
_ERROR = re.compile(r"^#(\d+) ERROR: (.*)$", re.MULTILINE)
# BuildKit numbers only these instructions in its progress (`[stage-0 3/6] RUN ...`).
_COUNTED = frozenset({"FROM", "RUN", "COPY", "ADD", "WORKDIR"})
STEPS = "sps-steps"  # label of the record: one digit per instruction, 1 when BuildKit counts it


def step_map(text: str) -> str | None:
    """The record's `sps-steps` label for a Dockerfile, or None when it is too long to keep."""
    bits = "".join("1" if s.keyword in _COUNTED else "0" for s in dockerfile.steps(text))
    return bits if len(bits) <= 255 else None


def _failed_step(log: str, bits: str | None) -> int | None:
    """The 1-based number of the customer's Dockerfile instruction whose own command failed, or
    None for a failure that no command of the customer's caused (a pull, a full disk).

    BuildKit's progress names a failed step by its place among the steps it runs, which counts
    only the instructions in `_COUNTED`; `bits` says which of the Dockerfile's instructions those
    are, so the count leads back to the instruction. A step past the customer's own (the tail that
    this adapter adds) has no number.
    """
    if not bits:
        return None
    counted = [i for i, bit in enumerate(bits, 1) if bit == "1"]
    for found in _ERROR.finditer(log):
        step_id, why = found[1], found[2]
        if not ("did not complete successfully" in why or "not found" in why):
            continue
        for head in _STEP.finditer(log):
            if head[1] == step_id and 1 <= int(head[2]) <= len(counted):
                return counted[int(head[2]) - 1]
    return None


def _error(native: dict[str, Any]) -> SpsError:
    """Why a build failed, in words of ours. `failed_step` is set only for a step of the customer's
    Dockerfile whose own command failed."""
    log = _log(native).lower()
    if "toomanyrequests" in log or "429 too many requests" in log or "pull rate limit" in log:
        return SpsError("rate_limited", "Blaxel's builder was rate limited pulling an image")
    pull = ("pull access denied", "manifest unknown", "failed to authorize", "not found: not found")
    if any(word in log for word in pull) and "did not complete successfully" not in log:
        return SpsError(
            "provider_error",
            "Blaxel's builder could not pull the base image; use a public image it can reach",
            field="source",
        )
    bits = ((native.get("metadata") or {}).get("labels") or {}).get(STEPS)
    step = _failed_step(_log(native), bits)
    if step is not None:
        return SpsError(
            "provider_error",
            f"The build failed at instruction {step} of the Dockerfile",
            failed_step=step,
        )
    return SpsError("provider_error", "Blaxel's image build failed")


def _labels(native: dict[str, Any]) -> dict[str, str]:
    labels = (native.get("metadata") or {}).get("labels") or {}
    return {k[len(USER) :]: v for k, v in labels.items() if k.startswith(USER)}


def _image_of(
    native: dict[str, Any], state: str, error: SpsError | None, size: int | None
) -> PreparedImage:
    return PreparedImage(
        id=native["metadata"]["name"],
        state=state,  # type: ignore[arg-type]
        error=error,
        resources=None,
        stored_bytes=size,
        labels=_labels(native),
        created_at=wire.stamp(native["metadata"]["createdAt"]),
    )


async def _prepared(p: BlaxelProvider, native: dict[str, Any]) -> PreparedImage:
    """An image's state: ready when Blaxel has built it, failed when the build failed, else
    preparing. A build that outlasts `BUILD_LIMIT_S` is failed."""
    name = native["metadata"]["name"]
    image = await _image(p, name)
    # Blaxel marks an image BUILT a moment before it lists its tag, and a sandbox starts from the
    # tag, so the image is ready when it has both.
    if (
        image is not None
        and (image.get("metadata") or {}).get("status") == "BUILT"
        and _tag(image) is not None
    ):
        size = (image.get("spec") or {}).get("size")
        return _image_of(native, "ready", None, size if isinstance(size, int) else None)
    if native["status"] in wire.ENDED or native["status"] == "FAILED":
        return _image_of(native, "failed", _error(native), None)
    age = datetime.now(UTC) - wire.stamp(native["metadata"]["createdAt"])
    if age > timedelta(seconds=BUILD_LIMIT_S):
        return _image_of(native, "failed", SpsError("timeout", "The build did not finish"), None)
    return _image_of(native, "preparing", None, None)


# -- operations ---------------------------------------------------------------------------


async def get(p: BlaxelProvider, image_id: str) -> PreparedImage:
    return await _prepared(p, await _record(p, image_id))


async def list_images(
    p: BlaxelProvider, labels: Mapping[str, str] | None, cursor: str | None, limit: int
) -> ImagePage:
    wanted = dict(labels or {})
    params: dict[str, Any] = {"limit": min(limit, 100)}
    while True:
        if cursor:
            params["cursor"] = cursor
        page = (await p.ctl("GET", "/sandboxes", "the image list", params=params)).json()
        meta = page.get("meta")
        cursor = meta.get("nextCursor") if isinstance(meta, dict) and meta.get("hasMore") else None
        found = []
        for native in page.get("data") or []:
            if not is_record(native) or _deleted(native):
                continue
            if any(_labels(native).get(k) != v for k, v in wanted.items()):
                continue
            if native["status"] in _BUILDING:
                found.append(await _prepared(p, native))  # Blaxel may have built it already
                continue
            state = "ready" if native["status"] == "DEPLOYED" else "failed"
            error = _error(native) if state == "failed" else None
            found.append(_image_of(native, state, error, None))
        if found or cursor is None:
            return ImagePage(images=found, next_cursor=cursor)


async def _settled(p: BlaxelProvider, name: str) -> None:
    """Wait until the record is no longer deploying."""
    deadline = asyncio.get_running_loop().time() + SETTLE_LIMIT_S
    while asyncio.get_running_loop().time() < deadline:
        native = await _native(p, name)
        if native is None or native["status"] not in _BUILDING:
            return
        await asyncio.sleep(0.5)
    raise SpsError("timeout", "The image's record did not settle", outcome="unknown")


async def sweep(p: BlaxelProvider) -> None:
    """Remove images of ours whose record is deleted and that nothing uses any more. Best effort: a
    failure here never fails the call that ran it."""
    try:
        resp = await p.ctl_raw("GET", "/images")
        if resp.status_code >= 400:
            return
        left = 0
        for found in resp.json():
            name = (found.get("metadata") or {}).get("name", "")
            if not _ID.fullmatch(name) or left >= SWEEP_LIMIT:
                continue
            record = await _native(p, name)
            if record is None or (is_record(record) and _deleted(record)):
                left += 1
                await p.ctl_raw("DELETE", f"/images/sandbox/{quote(name, safe='')}")
    except (SpsError, ValueError, AttributeError):
        return


async def delete(p: BlaxelProvider, image_id: str) -> ImageDeleteResult:
    """Delete the record and the image. A repeat succeeds while Blaxel still holds the record."""
    if not _ID.fullmatch(image_id):
        raise SpsError("not_found", "Blaxel holds no such image")
    native = await _native(p, image_id)
    if native is None or not is_record(native):
        raise SpsError("not_found", "Blaxel holds no such image")
    where = quote(image_id, safe="")
    repeat = _deleted(native)
    if native["status"] in _BUILDING:
        # Blaxel refuses to delete a sandbox that is still deploying; the image is built by then
        # and the deployment takes seconds, so wait for it rather than send the delete twice.
        await _settled(p, image_id)
    resp = await p.ctl_raw("DELETE", f"/sandboxes/{where}")
    if resp.status_code >= 400 and resp.status_code != 404:
        raise wire.failure(resp, "the image record")
    resp = await p.ctl_raw("DELETE", f"/images/sandbox/{where}")
    if resp.status_code == 400 and "in use" in resp.text:
        # A sandbox that started from the image still holds it. The record is already deleted, so
        # the image is gone to every operation here, and the next `prepare` removes the image
        # once nothing uses it (`sweep`).
        return ImageDeleteResult(id=image_id, state="deleted" if repeat else "deleting")
    if resp.status_code >= 400 and resp.status_code != 404:
        raise wire.failure(resp, "the image")
    return ImageDeleteResult(id=image_id, state="deleted")


async def resolve(p: BlaxelProvider, image_id: str) -> str:
    """The reference that `create` hands Blaxel for a prepared image: `sandbox/<name>:<tag>`."""
    native = await _record(p, image_id)
    image = await _image(p, image_id)
    if image is None or (image.get("metadata") or {}).get("status") != "BUILT":
        raise SpsError("conflict", "The image is not ready")
    tag = _tag(image)
    if tag is None:
        raise SpsError("conflict", "The image has no built tag")
    del native
    return f"sandbox/{image_id}:{tag}"


async def prepare(
    p: BlaxelProvider,
    request: ImageRequest,
    context: AsyncIterable[bytes] | None,
    context_size: int | None,
    key: str,
) -> PreparedImage:
    source = request.source
    if isinstance(source, PrivateOciSource):
        raise SpsError(
            "unsupported", "Blaxel's builder takes no registry credentials", field="source"
        )
    if len(request.labels) > MAX_LABELS - 1:
        raise SpsError("unsupported", f"A Blaxel image keeps at most {MAX_LABELS - 1} labels")
    if isinstance(source, OciSource):
        if context is not None:
            raise SpsError("invalid_request", "Only a Dockerfile takes a context", field="context")
        text = dockerfile.from_oci(source.oci, source.config, name_of(key))
    elif isinstance(source, DockerfileSource):
        text = dockerfile.from_dockerfile(
            source.dockerfile, source.base.config if source.base else None, name_of(key)
        )
    else:
        raise SpsError("invalid_request", "The source is an image or a Dockerfile", field="source")
    name = name_of(key)
    await sweep(p)
    spool, digest = await _spool(context, context_size)
    try:
        print_ = _fingerprint(request, digest)
        known = await _native(p, name)
        if known is not None:
            if not is_record(known) or known["metadata"].get("displayName") != print_:
                raise SpsError("conflict", "Idempotency-Key was used with a different request")
            if _deleted(known):
                raise SpsError("conflict", "The image was deleted; use a new Idempotency-Key")
            return await _prepared(p, known)
        archive = await asyncio.to_thread(_archive, text, spool)
    finally:
        if spool is not None:
            spool.close()
    try:
        labels = {"x-blaxel-auto-generated": "true", KIND: "image"}
        labels.update({f"{USER}{k}": v for k, v in request.labels.items()})
        if isinstance(source, DockerfileSource) and (bits := step_map(source.dockerfile)):
            labels[STEPS] = bits
        body = {
            "metadata": {"name": name, "displayName": print_, "labels": labels},
            "spec": {"runtime": {"memory": BUILD_MEMORY_MIB}},
        }
        where = f"/sandboxes/{quote(name, safe='')}"
        resp = await p.ctl_raw("PUT", where, params={"upload": "true"}, json=body)
        if resp.status_code == 404:
            resp = await p.ctl_raw("POST", "/sandboxes", params={"upload": "true"}, json=body)
        if resp.status_code >= 400:
            raise wire.failure(resp, "the image build")
        await p.upload(resp.headers.get("x-blaxel-upload-url"), archive)
    finally:
        archive.close()
    native = await _native(p, name)
    if native is None:
        raise SpsError("provider_error", "Blaxel did not record the image", outcome="unknown")
    return await _prepared(p, native)


def upload_target(url: str | None, allowed: tuple[str, ...]) -> str:
    """The presigned URL of Blaxel's build upload, refused unless it is an HTTPS host that the
    manifest names: it is the one place this adapter sends bytes to a host that Blaxel chose."""
    parts = urlsplit(url) if url else None
    if parts is None or parts.scheme != "https" or parts.hostname not in allowed:
        raise SpsError("provider_error", "Blaxel gave no usable upload address", outcome="unknown")
    assert url is not None
    return url
