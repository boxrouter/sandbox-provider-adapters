# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Commands as a named user, and exact output bytes, in a Blaxel sandbox.

Blaxel's process API takes one command string and answers with each stream as text, so a stream
with bytes that are not UTF-8 would be changed on the way. `run` with a `user` or with
`output: base64` therefore runs the command inside a wrapper (`WRAPPER`, a Bash script that gets
the command as its own arguments, so no quoting layer touches it):

- the command's streams go to two files in a private directory, and nothing else reaches Blaxel's
  text;
- the wrapper answers with the exit status and, for each stream, its size and its bytes in base64 (up
  to `INLINE` bytes); a larger stream stays in its file, and the adapter reads the first
  `output_limit_bytes` of it and removes the directory;
- a `user` is switched to by `SWITCH` (`setpriv` where it can set ids, else `su`, which is what
  Blaxel's Alpine image has), after the wrapper has found that the user exists.

A command that Blaxel's own timeout ends takes the wrapper with it; the adapter then reads what the
two files hold and removes them.
"""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING
from uuid import uuid4

from sandbox_provider_spec.v1 import FileInfo, RunRequest, RunResult, SpsError

if TYPE_CHECKING:
    from sps_provider_blaxel import BlaxelProvider

#: A stream of at most this many bytes comes back inside the answer; Blaxel's waited answer is JSON
#: text, and a larger one is read from its file.
INLINE = 32 * 1024
STAGE = "/tmp/.sps-run-"  # the private directory of one command is this plus a random hex
_USER = re.compile(r"[a-z_][a-z0-9_-]{0,31}|[0-9]{1,10}")
# The wrapper's own words, which no program prints as its first line.
NO_USER, NO_CWD, NO_TOOL, NO_STAGE = "SPS-NOUSER", "SPS-NOCWD", "SPS-NOTOOL", "SPS-NOSTAGE"

# `su` reads words that look like options after the user name as its own, so the user is the passwd
# name and the argument vector goes in as one command string with every word single-quoted.
_SU = r"""q=
for a in "$@"; do
  s=$a r=
  while :; do
    case $s in
    *\'*) r="$r${s%%\'*}'\\''"; s=${s#*\'} ;;
    *) break ;;
    esac
  done
  q="$q '$r$s'"
done
su -s /bin/sh -c : "$n" </dev/null >/dev/null 2>&1 || exit 121
exec su -s /bin/sh -c "exec$q" "$n"
"""

#: `/bin/sh -c SWITCH sps-user <user> <command...>`: HOME, USER and LOGNAME come from the user's
#: passwd entry and every other variable is inherited. It runs the command as the user, or ends with
#: 121 (the switch failed) before the command starts.
SWITCH = (
    """u=$1
shift
p=$(getent passwd "$u") || exit 126
IFS=: read -r n _ uid gid _ h _ <<EOF
$p
EOF
[ -n "$n" ] && [ -n "$uid" ] && [ -n "$gid" ] && [ -n "$h" ] || exit 126
HOME=$h USER=$n LOGNAME=$n
export HOME USER LOGNAME
if [ "$(id -u)" = "$uid" ] && [ "$(id -g)" = "$gid" ]; then exec "$@"; fi
if command -v setpriv >/dev/null 2>&1 &&
  setpriv --reuid="$uid" --regid="$gid" --init-groups -- /bin/sh -c : </dev/null >/dev/null 2>&1
then
  exec setpriv --reuid="$uid" --regid="$gid" --init-groups -- "$@"
fi
command -v su >/dev/null 2>&1 || exit 121
"""
    + _SU
)

#: `bash -c WRAPPER sps-run <dir> <cwd> <user> <limit> <inline> <switch> <command...>`
WRAPPER = r"""d=$1 cwd=$2 user=$3 limit=$4 inline=$5 sw=$6
shift 6
for t in base64 head wc tr mkdir; do
  command -v "$t" >/dev/null 2>&1 || { printf 'SPS-NOTOOL %s\n' "$t"; exit 0; }
done
if [ -n "$user" ]; then
  command -v getent >/dev/null 2>&1 || { printf 'SPS-NOTOOL getent\n'; exit 0; }
  getent passwd "$user" >/dev/null 2>&1 || { printf 'SPS-NOUSER\n'; exit 0; }
fi
if [ -n "$cwd" ]; then
  cd -- "$cwd" 2>/dev/null || { printf 'SPS-NOCWD\n'; exit 0; }
fi
mkdir -m 700 -- "$d" 2>/dev/null || { printf 'SPS-NOSTAGE\n'; exit 0; }
if [ -n "$user" ]; then
  /bin/sh -c "$sw" sps-user "$user" "$@" >"$d/o" 2>"$d/e" </dev/null
else
  "$@" >"$d/o" 2>"$d/e" </dev/null
fi
s=$?
printf '%s\n' "$s"
keep=
for f in o e; do
  n=$(wc -c <"$d/$f" | tr -d ' ')
  printf '%s ' "$n"
  if [ "$n" -le "$inline" ] || [ "$limit" -le "$inline" ]; then
    head -c "$limit" "$d/$f" | base64 | tr -d '\n'
  else
    keep=1
    printf '@'
  fi
  printf '\n'
done
if [ -n "$keep" ]; then printf 'KEEP\n'; else rm -rf -- "$d"; fi
"""


#: `/bin/sh -c COPY_AS sps-write <user> <staging file> <switch> <path>`: the staging file goes to the
#: user, who then copies it to the path.
COPY_AS = r"""chown "$1" -- "$2" || exit 1
exec /bin/sh -c "$3" sps-user "$1" /bin/sh -c \
  'mkdir -p -- "$(dirname -- "$2")" && cat -- "$1" >"$2"' sps-copy "$2" "$4"
"""


def check_user(user: str) -> None:
    if not _USER.fullmatch(user):
        raise SpsError("invalid_request", "The user must be a name or a numeric uid")


def switched(user: str, argv: list[str]) -> list[str]:
    """`argv` as one command that runs it as `user` (the caller has checked that the user exists)."""
    return ["/bin/sh", "-c", SWITCH, "sps-user", user, *argv]


def stage_dir() -> str:
    return f"{STAGE}{uuid4().hex}"


def wrapped(stage: str, request: RunRequest) -> list[str]:
    """The argument vector that runs `request` inside the wrapper."""
    return [
        "bash",
        "-c",
        WRAPPER,
        "sps-run",
        stage,
        request.cwd or "",
        request.user or "",
        str(request.output_limit_bytes),
        str(INLINE),
        SWITCH,
        *request.argv,
    ]


def _frame(line: str) -> tuple[int, bytes | None]:
    """One stream line, `<size> <base64>` or `<size> @` (kept in its file)."""
    size, _, rest = line.partition(" ")
    if not size.isdecimal():
        raise ValueError(line[:40])
    return int(size), (None if rest == "@" else base64.b64decode(rest, validate=True))


async def _remove(p: BlaxelProvider, sandbox_id: str, stage: str) -> None:
    """Best effort: what is left behind ends with the sandbox."""
    try:
        await p.quick(sandbox_id, ["rm", "-rf", "--", stage])
    except SpsError:
        pass


async def _staged(p: BlaxelProvider, sandbox_id: str, path: str, limit: int) -> bytes:
    """The first `limit` bytes of a kept stream."""
    stream = await p.files_read(sandbox_id, path)
    out = bytearray()
    try:
        async for chunk in stream.chunks:
            out += chunk
            if len(out) >= limit:
                break
    finally:
        await stream.chunks.aclose()  # type: ignore[attr-defined]
    return bytes(out[:limit])


def _result(
    request: RunRequest,
    code: int | None,
    out: bytes,
    err: bytes,
    out_size: int,
    err_size: int,
    *,
    timed_out: bool = False,
) -> RunResult:
    limit = request.output_limit_bytes
    cut, cut_err = out_size > limit, err_size > limit
    if request.output == "base64":
        return RunResult(
            exit_code=code,
            stdout="",
            stderr="",
            timed_out=timed_out,
            stdout_base64=base64.b64encode(out).decode(),
            stderr_base64=base64.b64encode(err).decode(),
            stdout_truncated=cut,
            stderr_truncated=cut_err,
        )
    return RunResult(
        exit_code=code,
        stdout=out.decode("utf-8", "replace"),
        stderr=err.decode("utf-8", "replace"),
        timed_out=timed_out,
        stdout_truncated=cut,
        stderr_truncated=cut_err,
    )


async def _after_timeout(
    p: BlaxelProvider, sandbox_id: str, stage: str, request: RunRequest
) -> RunResult:
    """What the two files hold when Blaxel's timeout ended the wrapper before it answered."""
    limit = request.output_limit_bytes
    try:
        out = await _staged(p, sandbox_id, f"{stage}/o", limit + 1)
        err = await _staged(p, sandbox_id, f"{stage}/e", limit + 1)
    except SpsError:
        out = err = b""
    await _remove(p, sandbox_id, stage)
    return _result(request, None, out[:limit], err[:limit], len(out), len(err), timed_out=True)


async def run(p: BlaxelProvider, sandbox_id: str, request: RunRequest) -> RunResult:
    """`run` for a request with a `user` or `output: base64`."""
    if request.user is not None:
        check_user(request.user)
    if not request.argv:
        raise SpsError("invalid_request", "argv must not be empty")
    stage = stage_dir()
    _, logs, timed_out = await p.execute(
        sandbox_id, wrapped(stage, request), request.env, request.timeout_ms
    )
    if timed_out:
        return await _after_timeout(p, sandbox_id, stage, request)
    text = str(logs["stdout"])
    first = text.split("\n", 1)[0]
    if first == NO_USER:
        raise SpsError("not_found", "The sandbox has no such user")
    if first == NO_CWD:
        raise SpsError("not_found", f"No such directory: {request.cwd}")
    if first.startswith(NO_TOOL) or first == NO_STAGE:
        raise SpsError(
            "provider_error", "The sandbox lacks a tool that exact output needs", outcome="failed"
        )
    lines = text.split("\n")
    if len(lines) < 3 or not first.lstrip("-").isdecimal():
        raise SpsError(
            "provider_error", "The command ended without its answer", outcome="unknown"
        ) from None
    try:
        out_size, out = _frame(lines[1])
        err_size, err = _frame(lines[2])
    except (ValueError, binascii.Error):
        raise SpsError(
            "provider_error", "The command's answer is unreadable", outcome="unknown"
        ) from None
    kept = len(lines) > 3 and lines[3] == "KEEP"
    try:
        limit = request.output_limit_bytes
        if out is None:
            out = await _staged(p, sandbox_id, f"{stage}/o", limit)
        if err is None:
            err = await _staged(p, sandbox_id, f"{stage}/e", limit)
    finally:
        if kept:
            await _remove(p, sandbox_id, stage)
    return _result(request, int(first), out, err, out_size, err_size)


async def write_as(
    p: BlaxelProvider,
    sandbox_id: str,
    path: str,
    data: AsyncIterable[bytes],
    size: int,
    user: str,
) -> FileInfo:
    """Write a file that `user` owns, with that user's permissions.

    The bytes go to a private staging file first, which the user then copies to `path` (creating
    missing parent directories), so the file is made by the user: it is owned by them, and it is
    refused where they may not write. The staging file is removed either way.
    """
    check_user(user)
    await p.require_user(sandbox_id, user)
    stage = f"{STAGE}{uuid4().hex}"
    await p.write_raw(sandbox_id, stage, data, size=size, mode="0600")
    try:
        code, err = await p.quick(
            sandbox_id, ["/bin/sh", "-c", COPY_AS, "sps-write", user, stage, SWITCH, path]
        )
    finally:
        await _remove(p, sandbox_id, stage)
    if code != 0:
        if "ermission denied" in err or "Read-only" in err:
            raise SpsError("forbidden", f"{user} may not write {path}")
        if "no space left" in err.lower():
            raise SpsError("too_large", "The sandbox's disk is full", outcome="unknown")
        raise SpsError("provider_error", f"The write as {user} failed", outcome="unknown")
    return FileInfo(path=path, size=size)
