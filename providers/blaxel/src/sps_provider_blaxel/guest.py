# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Commands as a named user, and exact output bytes, in a Blaxel sandbox.

Blaxel's process API takes one command string and answers with each stream as text, so a stream
with bytes that are not UTF-8 would be changed on the way. `run` with a `user` or with
`output: base64` therefore runs the command inside a short `sh` script (`WRAPPER`, which gets the
command as its own arguments, so no quoting layer touches it):

- the command's streams go to two files, `<stage>.o` and `<stage>.e`, and nothing else reaches
  Blaxel's text;
- the script answers with one header line (the exit status, each stream's size, `KEEP` or `-`)
  and the first `n` bytes of each stream, both in one base64 text: `n` is the smaller of
  `output_limit_bytes` and `INLINE`, and the sizes say where one stream ends. A stream over `INLINE`
  that the limit allows more of stays in its file (`KEEP`): the adapter reads the first
  `output_limit_bytes` of it and removes the files, and otherwise the script removes them;
- a `user` is switched to by `SWITCH` (`setpriv` where it can set ids, else `su`, which is what
  Blaxel's Alpine image has), after the script has found that the user exists.

A command that Blaxel's own timeout ends takes the script with it; the adapter then reads what the
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

#: `sh -c WRAPPER sps-run <stage> <cwd> <user> <n> <limit> [<switch>] <command...>`: the switch is
#: given only with a user. One process each for `wc`, `head`, `base64` and `rm` besides the command.
WRAPPER = r"""d=$1 c=$2 u=$3 n=$4 l=$5
shift 5
for t in base64 head wc rm; do
  command -v "$t" >/dev/null || { echo "SPS-NOTOOL $t"; exit 0; }
done
if [ -n "$u" ]; then
  command -v getent >/dev/null || { echo "SPS-NOTOOL getent"; exit 0; }
  getent passwd "$u" >/dev/null 2>&1 || { echo SPS-NOUSER; exit 0; }
  w=$1
  shift
fi
[ -z "$c" ] || cd -- "$c" 2>/dev/null || { echo SPS-NOCWD; exit 0; }
m=$(umask)
umask 077
{ true >"$d.o" && true >"$d.e"; } 2>/dev/null || { umask "$m"; echo SPS-NOSTAGE; exit 0; }
umask "$m"
if [ -n "$u" ]; then
  /bin/sh -c "$w" sps-user "$u" "$@" >"$d.o" 2>"$d.e" </dev/null
else
  "$@" >"$d.o" 2>"$d.e" </dev/null
fi
s=$?
set -- $(wc -c "$d.o" "$d.e")
k=-
[ "$l" -gt "$n" ] && { [ "$1" -gt "$n" ] || [ "$3" -gt "$n" ]; } && k=KEEP
echo "$s $1 $3 $k"
head -q -c "$n" "$d.o" "$d.e" | base64
[ "$k" = KEEP ] || rm -f "$d.o" "$d.e"
"""


#: `/bin/sh -c COPY_AS sps-write <user> <staging file> <switch> <path>`: the staging file goes to the
#: user, who then copies it to the path. The staging file is removed however this ends. It ends 126
#: for a user the sandbox does not have and 125 when the lookup itself failed.
COPY_AS = r"""trap 'rm -f -- "$2"' EXIT
getent passwd "$1" >/dev/null 2>&1
c=$?
[ "$c" -eq 0 ] || { [ "$c" -eq 2 ] && exit 126; exit 125; }
chown "$1" -- "$2" || exit 1
/bin/sh -c "$3" sps-user "$1" /bin/sh -c \
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
    limit = request.output_limit_bytes
    return [
        "sh",
        "-c",
        WRAPPER,
        "sps-run",
        stage,
        request.cwd or "",
        request.user or "",
        str(min(limit, INLINE)),
        str(limit),
        *([SWITCH] if request.user else []),
        *request.argv,
    ]


async def _remove(p: BlaxelProvider, sandbox_id: str, stage: str) -> None:
    """Best effort: what is left behind ends with the sandbox."""
    try:
        await p.quick(sandbox_id, ["rm", "-f", "--", f"{stage}.o", f"{stage}.e"])
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
        out = await _staged(p, sandbox_id, f"{stage}.o", limit + 1)
        err = await _staged(p, sandbox_id, f"{stage}.e", limit + 1)
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
        sandbox_id, wrapped(stage, request), request.env, request.timeout_ms, after_timeout=False
    )
    if timed_out:
        return await _after_timeout(p, sandbox_id, stage, request)
    header, _, rest = str(logs["stdout"]).partition("\n")
    words = header.split(" ")
    if words[0] == NO_USER:
        raise SpsError("not_found", "The sandbox has no such user")
    if words[0] == NO_CWD:
        raise SpsError("not_found", f"No such directory: {request.cwd}")
    if words[0] in (NO_TOOL, NO_STAGE):
        raise SpsError(
            "provider_error", "The sandbox lacks a tool that exact output needs", outcome="failed"
        )
    limit = request.output_limit_bytes
    n = min(limit, INLINE)
    try:
        if len(words) != 4 or not all(w.isdecimal() for w in words[:3]):
            raise ValueError(header[:40])
        code, out_size, err_size = (int(w) for w in words[:3])
        kept = words[3] == "KEEP"
        raw = base64.b64decode(rest)  # the lines of its wrapping are skipped
        out_at = min(out_size, n)
        if len(raw) != out_at + min(err_size, n):
            raise ValueError("size")
    except (ValueError, binascii.Error):
        raise SpsError(
            "provider_error", "The command ended without its answer", outcome="unknown"
        ) from None
    out, err = raw[:out_at], raw[out_at:]
    try:
        if out_size > n < limit:  # a stream over INLINE that the limit allows more of
            out = await _staged(p, sandbox_id, f"{stage}.o", limit)
        if err_size > n < limit:
            err = await _staged(p, sandbox_id, f"{stage}.e", limit)
    finally:
        if kept:
            await _remove(p, sandbox_id, stage)
    return _result(request, code, out, err, out_size, err_size)


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
    refused where they may not write. One command checks that the user exists, copies, and removes
    the staging file either way.
    """
    check_user(user)
    stage = f"{STAGE}{uuid4().hex}"
    await p.write_raw(sandbox_id, stage, data, size=size, mode="0600")
    try:
        code, err = await p.quick(
            sandbox_id, ["/bin/sh", "-c", COPY_AS, "sps-write", user, stage, SWITCH, path]
        )
    except SpsError:
        await _remove(p, sandbox_id, stage)  # the command may have ended before its own cleanup
        raise
    if code == 126:
        raise SpsError("not_found", "The sandbox has no such user")
    if code != 0:
        if "ermission denied" in err or "Read-only" in err:
            raise SpsError("forbidden", f"{user} may not write {path}")
        if "no space left" in err.lower():
            raise SpsError("too_large", "The sandbox's disk is full", outcome="unknown")
        raise SpsError("provider_error", f"The write as {user} failed", outcome="unknown")
    return FileInfo(path=path, size=size)
