# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""The Dockerfile that Blaxel builds a prepared image from.

Blaxel boots every sandbox through its own `sandbox-api` binary, which has to be the image's
entrypoint, so the customer's recipe is built with a tail of our own:

- the customer's instructions stay as they are and in their place, including their `ENTRYPOINT`
  and `CMD`, which the tail then overrides, so the number of an instruction in the build is the
  number in the customer's Dockerfile;
- `blaxel.toml` (`[build] slim = false`, which keeps Blaxel from slimming the image) is copied in
  and removed again;
- `sandbox-api` is copied from Blaxel's pinned image and made the entrypoint. A final `USER` that
  is not root is passed to it (`--user`), and the image's own startup command (its `ENTRYPOINT` and
  `CMD`, resolved as Docker does) goes to `--command`.

An OCI source is the same recipe over one `FROM`.
"""

from __future__ import annotations

import json
import re
import shlex
from dataclasses import dataclass

from sandbox_provider_spec.v1 import ImageConfig, SpsError

SANDBOX_API_IMAGE = "ghcr.io/blaxel-ai/sandbox"
SANDBOX_API_VERSION = "0.2.74"
SANDBOX_API_PATH = "/usr/local/bin/sandbox-api"
BLAXEL_TOML = "[build]\nslim = false\n"
_USER = re.compile(r"[a-zA-Z0-9_.-]+(?::[a-zA-Z0-9_.-]+)?")
_HEREDOC = re.compile(r"<<-?\s*[\"']?\w+")
_FROM = re.compile(r"(?:--\S+\s+)*(\S+)")


@dataclass(frozen=True)
class Step:
    """One instruction: its keyword, its value with continuations joined, and its text as written."""

    keyword: str
    value: str
    raw: str


def steps(dockerfile: str) -> list[Step]:
    """The instructions of a Dockerfile, in order. Blank lines and comments are dropped."""
    found: list[Step] = []
    lines: list[str] = []
    for line in dockerfile.splitlines():
        text = line.strip()
        if text.startswith("#") or (not lines and not text):
            continue
        lines.append(line)
        if text.endswith("\\"):
            continue
        found.append(_step(lines))
        lines = []
    if lines:
        found.append(_step(lines))
    return found


def _step(lines: list[str]) -> Step:
    joined = " ".join(part.strip().removesuffix("\\").strip() for part in lines)
    keyword, _, value = joined.partition(" ")
    step = Step(keyword.upper(), value.strip(), "\n".join(lines))
    if step.keyword in ("RUN", "COPY", "ADD") and _HEREDOC.search(step.value):
        raise SpsError(
            "unsupported", "Blaxel's builder does not take a here-document", field="source"
        )
    return step


def base_of(step: Step) -> str:
    """The image a `FROM` names, without its options and alias."""
    found = _FROM.match(step.value)
    if found is None:
        raise SpsError("invalid_request", "The FROM instruction names no image", field="source")
    return found[1]


def startup(structure: list[Step]) -> list[str] | None:
    """The image's startup command, from its last `ENTRYPOINT` and `CMD` as Docker resolves them."""
    entrypoint: str | None = None
    command: str | None = None
    shell = ["/bin/sh", "-c"]
    for step in structure:
        if step.keyword == "SHELL":
            parsed = _json_list(step.value)
            if not parsed:
                raise SpsError("invalid_request", "The SHELL instruction is not a JSON list")
            shell = parsed
        elif step.keyword == "ENTRYPOINT":
            entrypoint = step.value
        elif step.keyword == "CMD":
            command = step.value

    def argv(value: str) -> tuple[list[str], bool]:
        if value.lstrip().startswith("["):
            parsed = _json_list(value)
            if parsed is None:
                raise SpsError("invalid_request", "A startup command is not a JSON list")
            return parsed, True
        return [*shell, value], False

    if entrypoint is not None:
        first, exec_form = argv(entrypoint)
        if first:
            if exec_form and command is not None:
                first = first + argv(command)[0]
            return first
    return argv(command)[0] if command is not None else None


def _json_list(value: str) -> list[str] | None:
    try:
        parsed = json.loads(value)
    except ValueError:
        return None
    ok = isinstance(parsed, list) and all(isinstance(part, str) for part in parsed)
    return parsed if ok and parsed else None


def tail(user: str, command: list[str] | None, mark: str = "") -> list[str]:
    """The instructions that make the image boot through `sandbox-api`."""
    if user and not _USER.fullmatch(user):
        raise SpsError("invalid_request", "The image's USER is not a name or an id", field="source")
    argv = [SANDBOX_API_PATH]
    nonroot = bool(user and user.split(":")[0] not in {"root", "0"})
    if nonroot:
        argv += ["--user", user]
    if command:
        argv += ["--command", shlex.join(command)]
    # sandbox-api starts privileged and makes Go's cache in the inherited HOME before it serves a
    # command; making the parent as the image's own user keeps `~/.cache` from being root's.
    home = [f"USER {user}", 'RUN mkdir -p "$HOME/.cache"', "USER 0"] if nonroot else []
    # Blaxel reuses the build of an identical context, and the image that it then names has no tag,
    # so a label with the image's own name makes each build its own.
    label = [f"LABEL sps.image={mark}"] if mark else []
    return [
        *label,
        "COPY blaxel.toml /blaxel.toml",
        "USER 0",
        "RUN rm /blaxel.toml",
        *home,
        f"COPY --from={SANDBOX_API_IMAGE}:{SANDBOX_API_VERSION} /sandbox-api {SANDBOX_API_PATH}",
        "ENTRYPOINT " + json.dumps(argv),
        "CMD []",
    ]


def from_dockerfile(text: str, base: ImageConfig | None, mark: str = "") -> str:
    """The Dockerfile to build for a customer's recipe. `base` is the caller's resolution of the
    single `FROM`; Blaxel's builder reads no registry for us, so it supplies the image's user."""
    structure = steps(text)
    if sum(s.keyword == "FROM" for s in structure) != 1 or structure[0].keyword != "FROM":
        raise SpsError(
            "unsupported",
            "Blaxel builds a Dockerfile that starts with its one FROM instruction",
            field="source",
        )
    users = [s.value for s in structure if s.keyword == "USER"]
    user = users[-1] if users else (base.user if base and base.user else "")
    body = "\n".join(s.raw for s in structure)
    return body + "\n" + "\n".join(tail(user, startup(structure), mark)) + "\n"


def _quoted(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$") + '"'


def from_oci(reference: str, config: ImageConfig, mark: str = "") -> str:
    """The Dockerfile for an OCI image: its reference, the environment and directory that the caller
    resolved from it, and its own user, with `sandbox-api`."""
    lines = [f"FROM {reference}"]
    for key, value in config.env.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or "\n" in value:
            raise SpsError(
                "invalid_request", "An image's environment is names and values", field="source"
            )
        lines.append(f"ENV {key}={_quoted(value)}")
    if config.workdir:
        lines.append(f"WORKDIR {config.workdir}")
    return "\n".join([*lines, *tail(config.user or "", None, mark)]) + "\n"
