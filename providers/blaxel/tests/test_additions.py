# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Offline tests of the 1.0 additions: run as a user, exact bytes, images, previews, labels,
secrets, provider times and account cost, against a stand-in for Blaxel that really runs commands."""

from __future__ import annotations

import asyncio
import base64
import getpass
import io
import json
import subprocess
import tarfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
from sandbox_provider_spec.v1 import (
    CreateRequest,
    DockerfileSource,
    ImageRequest,
    OciSource,
    PrivateOciSource,
    RunRequest,
    Secret,
    SecretHeader,
    SpsError,
)
from sandbox_provider_spec.v1.types import ImageConfig, RegistryCredentials

from sps_provider_blaxel import BlaxelProvider, dockerfile, images

DATE = "Sat, 21 Nov 2026 03:00:00 GMT"
UPLOAD = "https://controlplane-prod-build-sources-us-east-2-fec15f.s3.us-east-2.amazonaws.com/u"
BUILD_LOG = (
    "the image build failed\n#6 [stage-0 1/6] FROM docker.io/library/alpine:3.20\n#6 DONE 0.2s\n"
    "#8 [stage-0 2/6] RUN echo step-a\n#8 DONE 0.1s\n"
    '#9 [stage-0 3/6] RUN echo boom >&2; false\n#9 ERROR: process "/bin/sh -c false" did not '
    "complete successfully: exit code: 1\n"
)


class Fake:
    """Blaxel's control plane and runtime. A process runs for real, through the local shell."""

    def __init__(self) -> None:
        self.sandboxes: dict[str, dict[str, Any]] = {}
        self.images: dict[str, dict[str, Any]] = {}
        self.previews: dict[str, dict[str, Any]] = {}
        self.uploads: list[bytes] = []
        self.requests: list[httpx.Request] = []
        self.metrics: list[dict[str, Any]] = []
        self.build_log: str | None = None  # set: a build fails with this log

    def provider(self) -> BlaxelProvider:
        return BlaxelProvider({"api_key": "key-1"}, transport=httpx.MockTransport(self.handle))

    @staticmethod
    def _json(body: Any, status: int = 200, **headers: str) -> httpx.Response:
        return httpx.Response(status, json=body, headers={"date": DATE, **headers})

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        host = request.url.host
        if host == "api.blaxel.ai":
            return self.control(request)
        if host.endswith(".amazonaws.com"):
            assert "x-blaxel-authorization" not in request.headers
            self.uploads.append(request.content)
            return httpx.Response(200)
        return self.runtime(request)

    def _add(self, body: dict[str, Any], upload: bool) -> dict[str, Any]:
        name = body["metadata"]["name"]
        record = {
            "status": "DEPLOYED",
            "events": [
                {
                    "type": "ai.blaxel.controlplane.api.create",
                    "status": "CREATED",
                    "time": "2026-11-21T02:00:00.123456789Z",
                },
                {
                    "type": "ai.blaxel.controlplane.deployment.succeeded",
                    "status": "DEPLOYED",
                    "time": "2026-11-21T02:00:05Z",
                },
            ],
            "lastUsedAt": "2026-11-21T02:10:00Z",
            "metadata": {
                **body["metadata"],
                "createdAt": "2026-11-21T02:00:00.123456789Z",
                "updatedAt": "2026-11-21T02:00:00Z",
                "url": f"https://sbx-{name}-x.us-pdx-1.bl.run",
            },
            "spec": {"region": "us-pdx-1", **body["spec"]},
        }
        if upload:
            record["status"] = "FAILED" if self.build_log else "DEPLOYED"
            if self.build_log:
                record["events"].append(
                    {
                        "type": "ai.blaxel.controlplane.buildimage.failed",
                        "status": "FAILED",
                        "message": self.build_log,
                        "time": "2026-11-21T02:00:30Z",
                    }
                )
            else:
                self.images[name] = {
                    "metadata": {"name": name, "status": "BUILT"},
                    "spec": {"size": 1234, "tags": [{"name": "tag1", "createdAt": "2026"}]},
                }
        self.sandboxes[name] = record
        return record

    def control(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path.removeprefix("/v0"), request.method
        if path == "/workspaces":
            return self._json([{"name": "ws-1"}])
        if path == "/accounts":
            return self._json([{"metadata": {"id": "acct-1"}}])
        if path == "/accounts/acct-1/metrics":
            return self._json({"data": self.metrics, "meta": {"hasMore": False}})
        upload = request.url.params.get("upload") == "true"
        if path == "/sandboxes" and method == "POST":
            body = json.loads(request.content)
            if body["metadata"]["name"] in self.sandboxes:
                return self._json({"code": "SANDBOX_ALREADY_EXISTS"}, 409)
            record = self._add(body, upload)
            return (
                self._json(record, **{"x-blaxel-upload-url": UPLOAD})
                if upload
                else self._json(record)
            )
        if path == "/sandboxes":
            return self._json({"data": list(self.sandboxes.values()), "meta": {"hasMore": False}})
        if path == "/images":
            return self._json(list(self.images.values()))
        if path.startswith("/images/sandbox/"):
            found = self.images.get(path.rsplit("/", 1)[1])
            if found is None:
                return self._json({"code": 404}, 404)
            if method == "DELETE":
                del self.images[path.rsplit("/", 1)[1]]
            return self._json(found)
        if "/previews" in path:
            return self.preview(path, method, request)
        name = path.removeprefix("/sandboxes/")
        record = self.sandboxes.get(name)
        if method == "PUT" and upload:
            body = json.loads(request.content)
            if record is None:
                return self._json({"code": 404}, 404)
            return self._json(record, **{"x-blaxel-upload-url": UPLOAD})
        if record is None:
            return self._json({"code": 404}, 404)
        if method == "PUT":
            body = json.loads(request.content)
            record["metadata"]["labels"] = body["metadata"]["labels"]
            record["spec"] = body["spec"]
        if method == "DELETE":
            record["status"] = "DELETING"
            record["events"].append(
                {"type": "ai.blaxel.controlplane.api.delete", "status": "DELETING"}
            )
        return self._json(record)

    def preview(self, path: str, method: str, request: httpx.Request) -> httpx.Response:
        parts = path.split("/previews")[1].strip("/").split("/")
        if method == "POST" and parts == [""]:
            body = json.loads(request.content)
            name = body["metadata"]["name"]
            self.previews[name] = {
                "metadata": {"name": name},
                "spec": {**body["spec"], "url": f"https://{name}.preview.bl.run"},
                "tokens": {},
            }
            return self._json(self.previews[name])
        found = self.previews.get(parts[0])
        if found is None:
            return self._json({"code": 404}, 404)
        if len(parts) == 2 and parts[1] == "tokens":
            body = json.loads(request.content)
            name = body["metadata"]["name"]
            found["tokens"][name] = body["spec"]
            return self._json(
                {"metadata": {"name": name}, "spec": {"token": "tok-secret", **body["spec"]}}
            )
        if len(parts) == 3:
            if method == "DELETE":
                found["tokens"].pop(parts[2], None)
            return self._json({})
        if method == "DELETE":
            del self.previews[parts[0]]
        return self._json(found)

    def runtime(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if path == "/process" and method == "POST":
            body = json.loads(request.content)
            done = subprocess.run(  # the command is the adapter's own quoted words
                ["/bin/sh", "-c", body["command"]], capture_output=True, check=False, timeout=60
            )
            self.last = body
            return self._json(
                {
                    "name": body["name"],
                    "status": "completed",
                    "exitCode": done.returncode,
                    "stdout": done.stdout.decode("utf-8", "replace"),
                    "stderr": done.stderr.decode("utf-8", "replace"),
                    "startedAt": DATE,
                }
            )
        if path.startswith("/filesystem/") and method == "GET":
            target = "/" + path.removeprefix("/filesystem/").lstrip("/")
            try:
                with open(target, "rb") as handle:
                    data = handle.read()
            except OSError:
                return self._json({"code": 404}, 404)
            return httpx.Response(
                200, content=data, headers={"content-disposition": 'attachment; filename="f"'}
            )
        return self._json({"code": 404}, 404)


@pytest.fixture
def fake() -> Fake:
    box = Fake()
    box.sandboxes["a" * 32] = box._add(
        {
            "metadata": {"name": "a" * 32, "labels": {"k": "1", "gone": "x"}},
            "spec": {"runtime": {"ttl": "300s"}},
        },
        False,
    )
    return box


SANDBOX = "a" * 32


def run(coro: Any) -> Any:
    return asyncio.run(coro)


async def _chunks(data: bytes) -> AsyncIterator[bytes]:
    for start in range(0, len(data), 7):
        yield data[start : start + 7]


def _shell(*words: str, **kw: Any) -> RunRequest:
    return RunRequest(argv=["bash", "-c", *words], **kw)


# -- exact bytes and users ---------------------------------------------------------------


def test_base64_output_is_exact(fake: Fake) -> None:
    result = run(
        fake.provider().run(
            SANDBOX, _shell("printf '\\xff\\x00ab'; printf '\\xfe' >&2; exit 3", output="base64")
        )
    )
    assert (result.exit_code, result.stdout, result.stderr) == (3, "", "")
    assert base64.b64decode(result.stdout_base64 or "") == b"\xff\x00ab"
    assert base64.b64decode(result.stderr_base64 or "") == b"\xfe"
    assert not (result.stdout_truncated or result.stderr_truncated)


def test_a_large_stream_is_read_from_its_file_and_cut_at_the_limit(fake: Fake) -> None:
    result = run(
        fake.provider().run(
            SANDBOX,
            _shell(
                "head -c 200000 /dev/zero | tr '\\0' x", output="base64", output_limit_bytes=100_000
            ),
        )
    )
    assert base64.b64decode(result.stdout_base64 or "") == b"x" * 100_000
    assert result.stdout_truncated and not result.stderr_truncated
    leftover = subprocess.run(["ls", "-a", "/tmp"], capture_output=True, check=True, text=True)
    assert ".sps-run-" not in leftover.stdout


def test_text_output_with_the_wrapper_replaces_bad_utf8(fake: Fake) -> None:
    result = run(fake.provider().run(SANDBOX, _shell("printf '\\xffz'", user=getpass.getuser())))
    assert result.stdout == "�z" and result.exit_code == 0


def test_a_missing_user_or_directory_is_not_found_and_nothing_runs(fake: Fake) -> None:
    box = fake.provider()
    with pytest.raises(SpsError) as error:
        run(box.run(SANDBOX, _shell("echo ran", user="no-such-user-xyz")))
    assert error.value.code == "not_found"
    with pytest.raises(SpsError) as error:
        run(box.run(SANDBOX, _shell("echo ran", output="base64", cwd="/no/such/dir")))
    assert error.value.code == "not_found"
    with pytest.raises(SpsError) as error:
        run(box.run(SANDBOX, _shell("true", user="Bad User!")))
    assert error.value.code == "invalid_request"


def test_the_working_directory_applies_inside_the_wrapper(fake: Fake) -> None:
    result = run(fake.provider().run(SANDBOX, _shell("pwd", output="base64", cwd="/usr")))
    assert base64.b64decode(result.stdout_base64 or "") == b"/usr\n"


# -- labels, times, secrets, cost --------------------------------------------------------


def test_labels_update_sets_and_removes_and_keeps_the_rest(fake: Fake) -> None:
    box = fake.provider()
    updated = run(box.labels_update(SANDBOX, {"new": "2", "gone": ""}))
    assert dict(updated.labels) == {"k": "1", "new": "2"}
    assert fake.sandboxes[SANDBOX]["spec"]["runtime"]["ttl"] == "300s"
    with pytest.raises(SpsError) as error:
        run(box.labels_update(SANDBOX, {f"k{i}": "v" for i in range(40)}))
    assert error.value.code == "invalid_request"
    fake.sandboxes[SANDBOX]["status"] = "DELETING"
    with pytest.raises(SpsError) as error:
        run(box.labels_update(SANDBOX, {"a": "b"}))
    assert error.value.code == "not_found"


def test_get_returns_the_providers_own_times(fake: Fake) -> None:
    times = {(t.kind, t.at.isoformat()) for t in run(fake.provider().get(SANDBOX)).times}
    assert ("created", "2026-11-21T02:00:00.123456+00:00") in times
    assert ("ready", "2026-11-21T02:00:05+00:00") in times
    assert ("deadline", "2026-11-21T02:05:00.123456+00:00") in times
    assert ("ready", "2026-11-21T02:10:00+00:00") in times  # lastUsedAt


def test_secrets_become_proxy_routes_and_never_leave_in_a_message(fake: Fake) -> None:
    box = fake.provider()
    secret = Secret(
        "ph", "s3cret-value", ["api.example.com"], [SecretHeader("Authorization", "Bearer ")]
    )
    sandbox = run(
        box.create(
            CreateRequest(secrets=[secret], requirements={"hidden_secrets": {}}),
            idempotency_key="k1",
        )
    )
    network = fake.sandboxes[sandbox.id]["spec"]["network"]
    assert network["firewall"] == {"rulesets": ["proxy"]}
    route = network["proxy"]["routing"][0]
    assert route["destinations"] == ["api.example.com"]
    assert route["headers"] == {"Authorization": "Bearer {{SECRET:sps-secret-0}}"}
    assert route["secrets"] == {"sps-secret-0": "s3cret-value"}
    assert "s3cret-value" not in repr(sandbox)
    with pytest.raises(SpsError) as error:
        run(box.create(CreateRequest(secrets=[secret]), idempotency_key="k2"))
    assert error.value.code == "invalid_request" and "s3cret" not in str(error.value)
    wild = Secret("ph", "s3cret-value", ["*.example.com"], [SecretHeader("X")])
    with pytest.raises(SpsError) as error:
        run(
            box.create(
                CreateRequest(secrets=[wild], requirements={"hidden_secrets": {}}),
                idempotency_key="k3",
            )
        )
    assert error.value.code == "unsupported" and "s3cret" not in str(error.value)
    with pytest.raises(SpsError) as error:
        run(
            box.create(
                CreateRequest(requirements={"hidden_secrets": {"scrubs_responses": True}}),
                idempotency_key="k4",
            )
        )
    assert error.value.code == "unsupported"


def _rows(*costs: tuple[str, str], name: str = SANDBOX) -> list[dict[str, Any]]:
    return [
        {
            "resourceType": "sandbox",
            "resourceName": name,
            "timeseries": [{"timestamp": ts, "cost": cost} for ts, cost in costs],
        }
    ]


def test_account_cost_adds_every_resource_inside_the_window(fake: Fake) -> None:
    fake.metrics = [
        *_rows(("2026-11-21T00:00:00Z", "0.5"), ("2026-11-21T01:00:00Z", "0.25")),
        *_rows(("2026-11-21T01:00:00Z", "1"), ("2026-11-20T23:00:00Z", "9")),
    ]
    start = datetime(2026, 11, 21, 0, tzinfo=UTC)
    got = run(fake.provider().account_cost(start, datetime(2026, 11, 21, 2, tzinfo=UTC)))
    assert got.total_usd == Decimal("1.75") and got.source == "reported"
    assert [(l.start.hour, l.amount_usd) for l in got.lines if l.start] == [
        (0, Decimal("0.5")),
        (1, Decimal("1.25")),
    ]
    assert got.status == "final"  # Blaxel's clock is an hour past the window
    for bad in ((start, start), (start, datetime(2026, 12, 1, tzinfo=UTC))):
        with pytest.raises(SpsError) as error:
            run(fake.provider().account_cost(*bad))
        assert error.value.code == "invalid_request"


def test_cost_of_a_sandbox_whose_record_is_gone_starts_at_the_hint(fake: Fake) -> None:
    fake.metrics = _rows(("2026-11-21T01:00:00Z", "0.5"), name="b" * 32)
    box = fake.provider()
    with pytest.raises(SpsError) as error:
        run(box.cost("b" * 32))
    assert error.value.code == "not_found"
    got = run(box.cost("b" * 32, created_at=datetime(2026, 11, 21, 1, 20, tzinfo=UTC)))
    assert got.total_usd == Decimal("0.5") and got.status == "accruing"  # its end is not known


# -- previews --------------------------------------------------------------------------


def test_public_and_private_previews_and_their_revocation(fake: Fake) -> None:
    box = fake.provider()
    public = run(box.preview_link(SANDBOX, 3000))
    assert (public.access, public.credential, public.id) == ("public", None, "sps-3000-public")
    private = run(box.preview_link(SANDBOX, 3000, access="private", lifetime_seconds=120))
    assert private.credential == "link" and private.url.endswith("?bl_preview_token=tok-secret")
    assert private.headers == {"X-Blaxel-Preview-Token": "tok-secret"}
    assert (
        private.expires_at is not None and private.id and private.id.startswith("sps-3000-private/")
    )
    assert run(box.preview_link(SANDBOX, 3000)).url == public.url  # one preview per port and mode
    run(box.preview_revoke(SANDBOX, private.id))
    run(box.preview_revoke(SANDBOX, private.id))
    assert not fake.previews["sps-3000-private"]["tokens"] and "sps-3000-public" in fake.previews
    run(box.preview_revoke(SANDBOX, public.id or ""))
    run(box.preview_revoke(SANDBOX, public.id or ""))
    assert "sps-3000-public" not in fake.previews
    with pytest.raises(SpsError) as error:
        run(box.preview_link(SANDBOX, 8080))
    assert error.value.code == "unsupported"
    with pytest.raises(SpsError) as error:
        run(box.preview_revoke(SANDBOX, "not-ours"))
    assert error.value.code == "not_found"


# -- images ------------------------------------------------------------------------------


ALPINE = "docker.io/library/alpine:3.20@sha256:" + "a" * 64


def _tar(files: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return out.getvalue()


def _zip_names(data: bytes) -> dict[str, bytes]:
    import zipfile

    with zipfile.ZipFile(io.BytesIO(data)) as z:
        return {n: z.read(n) for n in z.namelist()}


def test_a_dockerfile_image_is_built_labeled_and_idempotent(fake: Fake) -> None:
    box = fake.provider()
    context = _tar({"copied.txt": b"hello", "sub/b.bin": bytes(range(256))})
    request = ImageRequest(
        DockerfileSource(f"FROM {ALPINE}\nENV A=1\nRUN echo hi\nCOPY copied.txt /x\n"),
        labels={"run": "r1"},
    )
    first = run(
        box.image_prepare(
            request, context=_chunks(context), context_size=len(context), idempotency_key="i1"
        )
    )
    assert (
        first.id.startswith("sps-img-") and first.state == "ready" and first.labels == {"run": "r1"}
    )
    assert first.stored_bytes == 1234
    zipped = _zip_names(fake.uploads[0])
    assert zipped["sub/b.bin"] == bytes(range(256)) and zipped["blaxel.toml"].startswith(b"[build]")
    text = zipped["Dockerfile"].decode()
    assert text.startswith(
        f"FROM {ALPINE}\nENV A=1\nRUN echo hi\nCOPY copied.txt /x\nLABEL sps.image=sps-img-"
    )
    assert f"LABEL sps.image={first.id}\n" in text  # an identical build is never reused
    assert "/usr/local/bin/sandbox-api" in text and text.rstrip().endswith("CMD []")
    record = fake.sandboxes[first.id]["metadata"]
    assert record["labels"]["l.run"] == "r1" and record["labels"]["sps-kind"] == "image"
    assert record["labels"]["sps-steps"] == "1011"  # FROM, ENV, RUN, COPY: BuildKit counts three
    again = run(
        box.image_prepare(
            request, context=_chunks(context), context_size=len(context), idempotency_key="i1"
        )
    )
    assert again.id == first.id and len(fake.uploads) == 1
    with pytest.raises(SpsError) as error:
        run(box.image_prepare(ImageRequest(DockerfileSource("FROM x\n")), idempotency_key="i1"))
    assert error.value.code == "conflict"
    assert [i.id for i in run(box.image_list(labels={"run": "r1"})).images] == [first.id]
    assert run(box.image_list(labels={"run": "other"})).images == []
    sandboxes = run(box.list()).sandboxes
    assert [s.id for s in sandboxes] == [SANDBOX]  # the image's record is no sandbox
    for call in (box.get, box.destroy):
        with pytest.raises(SpsError) as error:
            run(call(first.id))
        assert error.value.code == "not_found"


def test_create_from_a_prepared_image_uses_its_tag_and_a_deleted_image_is_gone(fake: Fake) -> None:
    box = fake.provider()
    image = run(
        box.image_prepare(
            ImageRequest(OciSource(ALPINE, "sha256:" + "a" * 64, ImageConfig(user="root"))),
            idempotency_key="i2",
        )
    )
    sandbox = run(
        box.create(
            CreateRequest(
                image={"provider": "blaxel", "provider_artifact": image.id},
                requirements={"custom_images": {}},
            ),
            idempotency_key="c1",
        )
    )
    assert fake.sandboxes[sandbox.id]["spec"]["runtime"]["image"] == f"sandbox/{image.id}:tag1"
    assert run(box.image_delete(image.id)).state == "deleted"
    assert run(box.image_delete(image.id)).state == "deleted"
    assert image.id not in fake.images
    with pytest.raises(SpsError) as error:
        run(box.image_get(image.id))
    assert error.value.code == "not_found"
    with pytest.raises(SpsError) as error:
        run(
            box.create(
                CreateRequest(
                    image={"provider": "blaxel", "provider_artifact": image.id},
                    requirements={"custom_images": {}},
                ),
                idempotency_key="c2",
            )
        )
    assert error.value.code == "not_found"
    assert run(box.image_list()).images == []
    with pytest.raises(SpsError) as error:
        run(box.image_delete("sps-img-" + "0" * 32))
    assert error.value.code == "not_found"


def test_a_failed_build_names_the_customers_failing_step(fake: Fake) -> None:
    fake.build_log = BUILD_LOG
    source = f"FROM {ALPINE}\nENV A=1\nRUN echo step-a\nLABEL x=y\nRUN echo boom >&2; false\n"
    got = run(
        fake.provider().image_prepare(ImageRequest(DockerfileSource(source)), idempotency_key="f1")
    )
    assert got.state == "failed" and got.error is not None
    assert got.error.failed_step == 5  # the fifth instruction; BuildKit's own count was three
    assert "boom" not in got.error.message


@pytest.mark.parametrize(
    ("log", "code", "step"),
    [
        ("#2 ERROR: pull access denied, repository does not exist", "provider_error", None),
        ("#2 ERROR: 429 Too Many Requests toomanyrequests", "rate_limited", None),
        ("#4 ERROR: no space left on device", "provider_error", None),
    ],
)
def test_a_failure_that_is_not_the_customers_step_has_no_failed_step(
    fake: Fake, log: str, code: str, step: int | None
) -> None:
    fake.build_log = log
    got = run(
        fake.provider().image_prepare(
            ImageRequest(DockerfileSource(f"FROM {ALPINE}\n")), idempotency_key="f2"
        )
    )
    assert got.error is not None and got.error.code == code and got.error.failed_step == step


def test_unbuildable_requests_are_refused_before_anything_is_sent(fake: Fake) -> None:
    box = fake.provider()
    creds = RegistryCredentials("u", "p")
    refused = [
        (
            ImageRequest(PrivateOciSource(ALPINE, "sha256:" + "a" * 64, ImageConfig(), creds)),
            None,
            "unsupported",
        ),
        (ImageRequest(DockerfileSource("FROM a\nFROM b\n")), None, "unsupported"),
        (ImageRequest(DockerfileSource("RUN true\n")), None, "unsupported"),
        (ImageRequest(DockerfileSource("FROM a\nRUN <<EOF\ntrue\nEOF\n")), None, "unsupported"),
        (
            ImageRequest(OciSource(ALPINE, "sha256:" + "a" * 64, ImageConfig())),
            _tar({"a": b"x"}),
            "invalid_request",
        ),
        (ImageRequest(DockerfileSource("FROM a\n")), _tar({"blaxel.toml": b"x"}), "unsupported"),
        (ImageRequest(DockerfileSource("FROM a\n")), _tar({"../escape": b"x"}), "invalid_request"),
        (ImageRequest(DockerfileSource("FROM a\n")), b"not a tar archive" * 40, "invalid_request"),
    ]
    for request, context, code in refused:
        with pytest.raises(SpsError) as error:
            run(
                box.image_prepare(
                    request,
                    context=None if context is None else _chunks(context),
                    idempotency_key="r1",
                )
            )
        assert error.value.code == code
    assert not fake.uploads and not [r for r in fake.requests if r.method in ("PUT", "POST")]


def test_the_build_is_uploaded_only_to_the_host_the_manifest_names(fake: Fake) -> None:
    with pytest.raises(SpsError):
        images.upload_target("https://evil.example.com/u", BlaxelProvider.api_domains)
    with pytest.raises(SpsError):
        images.upload_target(
            "http://" + UPLOAD.removeprefix("https://"), BlaxelProvider.api_domains
        )
    assert images.upload_target(UPLOAD, BlaxelProvider.api_domains) == UPLOAD


# -- the Dockerfile ---------------------------------------------------------------------


def test_the_tail_makes_sandbox_api_the_entrypoint_and_passes_user_and_startup() -> None:
    text = dockerfile.from_dockerfile(
        'FROM a\nUSER app\nENTRYPOINT ["/e"]\nCMD ["--x"]\n', ImageConfig(user="root")
    )
    assert text.startswith('FROM a\nUSER app\nENTRYPOINT ["/e"]\nCMD ["--x"]\nCOPY blaxel.toml')
    entry = [line for line in text.splitlines() if line.startswith("ENTRYPOINT")][-1]
    assert json.loads(entry.removeprefix("ENTRYPOINT ")) == [
        "/usr/local/bin/sandbox-api",
        "--user",
        "app",
        "--command",
        "/e --x",
    ]
    assert 'RUN mkdir -p "$HOME/.cache"' in text
    root = dockerfile.from_oci("img@sha256:1", ImageConfig(user="0"))
    assert "--user" not in root and root.startswith("FROM img@sha256:1\n")
    from_base = dockerfile.from_dockerfile("FROM a\n", ImageConfig(user="nobody"))
    assert '"--user", "nobody"' in from_base
    with pytest.raises(SpsError):
        dockerfile.tail("bad user; rm", None)


def test_continued_instructions_are_one_step_and_comments_are_dropped() -> None:
    parsed = dockerfile.steps("# c\nFROM a\nRUN a \\\n  # inner\n  && b\n\nENV X=1\n")
    assert [s.keyword for s in parsed] == ["FROM", "RUN", "ENV"]
    assert parsed[1].value == "a && b"


def test_a_provider_artifact_image_is_used_without_naming_the_section(fake: Fake) -> None:
    """A request with an `image` and no `custom_images` requirement still starts from the image,
    and the image's `provider` may be left out."""
    got = run(
        fake.provider().create(
            CreateRequest(image={"provider_artifact": "sandbox/x:t1"}), idempotency_key="pa1"
        )
    )
    assert fake.sandboxes[got.id]["spec"]["runtime"]["image"] == "sandbox/x:t1"
    with pytest.raises(SpsError) as error:
        run(
            fake.provider().create(
                CreateRequest(image={"provider": "other-provider", "provider_artifact": "x"}),
                idempotency_key="pa2",
            )
        )
    assert error.value.code == "invalid_request"
    with pytest.raises(SpsError) as error:
        run(
            fake.provider().create(
                CreateRequest(
                    image={"provider_artifact": "x"},
                    requirements={"custom_images": {"runs_entrypoint": True}},
                ),
                idempotency_key="pa3",
            )
        )
    assert error.value.code == "unsupported"


def test_deleting_an_image_that_a_sandbox_still_uses_is_accepted_and_swept_later(
    fake: Fake,
) -> None:
    box = fake.provider()
    first = run(box.image_prepare(ImageRequest(DockerfileSource("FROM a\n")), idempotency_key="u1"))
    held = fake.images[first.id]
    removed: list[str] = []
    original = fake.control

    def in_use(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if request.method == "DELETE" and request.url.path.startswith("/v0/images/sandbox/"):
            if held["busy"]:
                return fake._json({"code": 400, "error": "image in use"}, 400)
            removed.append(name)
        return original(request)

    held["busy"] = True
    fake.control = in_use  # type: ignore[method-assign]
    assert run(box.image_delete(first.id)).state == "deleting"
    with pytest.raises(SpsError) as error:
        run(box.image_get(first.id))
    assert error.value.code == "not_found"
    held["busy"] = False
    run(box.image_prepare(ImageRequest(DockerfileSource("FROM b\n")), idempotency_key="u2"))
    assert removed == [first.id]


def test_an_image_is_ready_only_when_blaxel_lists_its_tag(fake: Fake) -> None:
    box = fake.provider()
    made = run(box.image_prepare(ImageRequest(DockerfileSource("FROM a\n")), idempotency_key="t1"))
    tags = fake.images[made.id]["spec"]["tags"]
    fake.images[made.id]["spec"]["tags"] = None  # BUILT a moment before the tag is listed
    fake.sandboxes[made.id]["status"] = "DEPLOYING"
    fake.sandboxes[made.id]["metadata"]["createdAt"] = datetime.now(UTC).isoformat()
    assert run(box.image_get(made.id)).state == "preparing"
    with pytest.raises(SpsError) as error:
        run(box.create(CreateRequest(image={"provider_artifact": made.id}), idempotency_key="t1s"))
    assert error.value.code == "conflict"
    fake.images[made.id]["spec"]["tags"] = tags
    assert run(box.image_get(made.id)).state == "ready"
