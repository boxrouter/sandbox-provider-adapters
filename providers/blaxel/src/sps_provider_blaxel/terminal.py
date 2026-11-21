# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Blaxel's terminal WebSocket as a 1.0 `Terminal`.

A sandbox's runtime host serves `wss://<host>/terminal/ws?cols=&rows=&sessionId=` (the optional
`workingDir` sets the directory and `shell` names the program to run on the pseudo-terminal). Both
directions carry JSON text frames: `{"type": "input", "data": str}` and
`{"type": "resize", "cols", "rows"}` to Blaxel, `{"type": "output", "data": str}` from it. When the
program ends Blaxel closes the socket with code 1000 and the reason "Shell exited", and sends no
exit status. So the program runs inside a small launcher script that sets `TERM` and the
environment, runs the program as the contract asks (an argv, or the default login shell) and writes
`$?` to a file; the adapter reads that file once the socket has closed. Blaxel keeps the shell when a
client leaves, so closing a terminal whose program has not exited sends SIGHUP to the processes of
its session and, five seconds later, SIGKILL.
"""

from __future__ import annotations

import codecs
import json
import re
import shlex
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import Any
from urllib.parse import quote, urlencode
from uuid import uuid4

from sandbox_provider_spec.v1 import SpsError, Terminal
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

SHELL_EXITED = (1000, "Shell exited")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# What a login shell is, when the request names no program: bash where the image has it.
_LOGIN_SHELL = "if command -v bash >/dev/null 2>&1; then bash -l; else sh -l; fi"


def launcher(
    argv: Sequence[str] | None,
    env: Mapping[str, str],
    term: str,
    status_path: str,
    pid_path: str,
) -> bytes:
    """The script that runs the terminal's program and records its pid and its exit status.

    Blaxel starts the script as the leader of its session, so its pid names the terminal's session
    (a job-control shell is a group of its own inside it). The script handles SIGHUP, so that it
    stays until its program has ended and reaps it: a program left to Blaxel's init stays a zombie.
    """
    for name in env:
        if not _ENV_NAME.fullmatch(name):
            raise SpsError("invalid_request", f"{name!r} is not an environment variable name")
    lines = ["#!/bin/sh", f"echo $$ > {shlex.quote(pid_path)}", "trap : HUP"]
    lines += [f"export TERM={shlex.quote(term)}"]
    lines += [f"export {name}={shlex.quote(value)}" for name, value in env.items()]
    lines += [shlex.join(argv) if argv else _LOGIN_SHELL, "code=$?"]
    lines += [f"echo $code > {shlex.quote(status_path)}", 'exit "$code"']
    return ("\n".join(lines) + "\n").encode()


def socket_query(columns: int, rows: int, script: str, cwd: str | None) -> str:
    query: dict[str, Any] = {
        "cols": columns,
        "rows": rows,
        "sessionId": uuid4().hex,
        "shell": script,
    }
    if cwd is not None:
        query["workingDir"] = cwd
    return urlencode(query, quote_via=quote)


class BlaxelTerminal(Terminal):
    """An open terminal. `read_status` and `cleanup` act on the sandbox, not on the socket."""

    def __init__(
        self,
        socket: ClientConnection,
        read_status: Callable[[], Awaitable[int | None]],
        cleanup: Callable[[bool], Awaitable[None]],
    ) -> None:
        self.exit_code: int | None = None
        self._socket = socket
        self._read_status = read_status
        self._cleanup = cleanup
        self._input = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._closed = False

    async def write(self, data: bytes) -> None:
        text = self._input.decode(data)
        if not text:
            return
        await self._send({"type": "input", "data": text})

    async def resize(self, columns: int, rows: int) -> None:
        await self._send({"type": "resize", "cols": columns, "rows": rows})

    async def _send(self, message: dict[str, Any]) -> None:
        try:
            await self._socket.send(json.dumps(message, separators=(",", ":")))
        except ConnectionClosed:
            raise SpsError("conflict", "The terminal has ended") from None

    async def output(self) -> AsyncIterator[bytes]:
        try:
            async for message in self._socket:
                if not isinstance(message, str):
                    raise SpsError("provider_error", "Blaxel sent a binary terminal frame")
                found = json.loads(message)
                kind, data = found.get("type"), found.get("data")
                if kind == "output" and isinstance(data, str):
                    yield data.encode()
                elif kind == "error":
                    raise SpsError("provider_error", "Blaxel reported a terminal error")
        except ConnectionClosed:
            pass
        if (self._socket.close_code, self._socket.close_reason) == SHELL_EXITED:
            self.exit_code = await self._read_status()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._socket.close()
        finally:
            # A program that has not exited is ended with the terminal (contract section 16).
            await self._cleanup(self.exit_code is None)
