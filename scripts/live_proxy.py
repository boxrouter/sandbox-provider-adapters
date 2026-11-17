# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: Apache-2.0
"""A CONNECT proxy for the live suite that allows only a provider's declared hosts.

It reads `api_domains` from a provider's `sps-provider.yaml`: an entry is an exact host, or a
`.suffix` that matches the suffix's subdomains and the bare domain. It tunnels `CONNECT host:port`
to an allowed host and answers 403 to every other target, logging each refusal. It does not
intercept TLS. On a GitHub-hosted runner it enforces the host allowlist only; refusing private
addresses matters in AWS, where the egress proxy does it.

    python scripts/live_proxy.py --manifest providers/<p>/sps-provider.yaml --port 3128
"""

import argparse
import asyncio
import contextlib
import signal
import sys
from pathlib import Path

import yaml

MAX_HEADER_BYTES = 16384
CONNECT_TIMEOUT_S = 15.0
IDLE_TIMEOUT_S = 600.0


def read_allowlist(manifest: Path) -> tuple[frozenset[str], tuple[str, ...]]:
    """The exact hosts and the `.suffix` entries declared in a provider manifest."""
    domains = (yaml.safe_load(manifest.read_text(encoding="utf-8")) or {}).get("api_domains") or []
    if not isinstance(domains, list) or not all(isinstance(item, str) for item in domains):
        raise SystemExit(f"{manifest}: api_domains must be a list of strings")
    cleaned = [item.strip().lower().rstrip(".") for item in domains]
    hosts = frozenset(item for item in cleaned if item and not item.startswith("."))
    suffixes = tuple(item for item in cleaned if item.startswith("."))
    return hosts, suffixes


def allowed(host: str, hosts: frozenset[str], suffixes: tuple[str, ...]) -> bool:
    """Whether `host` is declared, as an exact host or under a declared suffix."""
    host = host.lower().rstrip(".")
    if host in hosts:
        return True
    return any(host == suffix[1:] or host.endswith(suffix) for suffix in suffixes)


def parse_target(text: str) -> tuple[str, int] | None:
    """`host:port` from a CONNECT line, or None when it is malformed or an IP-literal bracket."""
    host, separator, port = text.rpartition(":")
    if not separator or not host or host.startswith("[") or not port.isdigit():
        return None
    number = int(port)
    return (host, number) if 0 < number < 65536 else None


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Copy bytes until the reader closes or goes idle, then close the writer."""
    try:
        while data := await asyncio.wait_for(reader.read(65536), IDLE_TIMEOUT_S):
            writer.write(data)
            await writer.drain()
    except (OSError, asyncio.TimeoutError):
        pass
    finally:
        with contextlib.suppress(OSError):
            writer.close()


def refuse(writer: asyncio.StreamWriter, status: str) -> None:
    writer.write(f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".encode())
    writer.close()


def make_handler(hosts: frozenset[str], suffixes: tuple[str, ...]):
    async def handle(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            head = await asyncio.wait_for(client_reader.readuntil(b"\r\n\r\n"), CONNECT_TIMEOUT_S)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError):
            client_writer.close()
            return
        if len(head) > MAX_HEADER_BYTES:
            return refuse(client_writer, "431 Request Header Fields Too Large")
        parts = head.split(b"\r\n", 1)[0].decode("latin-1").split()
        if len(parts) != 3 or parts[0] != "CONNECT":
            return refuse(client_writer, "405 Method Not Allowed")
        target = parse_target(parts[1])
        if target is None:
            return refuse(client_writer, "400 Bad Request")
        host, port = target
        if not allowed(host, hosts, suffixes):
            print(f"live_proxy: refused {host}:{port}", file=sys.stderr, flush=True)
            return refuse(client_writer, "403 Forbidden")
        try:
            upstream_reader, upstream_writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), CONNECT_TIMEOUT_S
            )
        except (OSError, asyncio.TimeoutError) as error:
            print(f"live_proxy: cannot reach {host}:{port}: {error}", file=sys.stderr, flush=True)
            return refuse(client_writer, "502 Bad Gateway")
        print(f"live_proxy: allowed {host}:{port}", file=sys.stderr, flush=True)
        client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await client_writer.drain()
        await asyncio.gather(
            pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer)
        )

    return handle


async def serve(manifest: Path, port: int) -> None:
    hosts, suffixes = read_allowlist(manifest)
    server = await asyncio.start_server(
        make_handler(hosts, suffixes), "127.0.0.1", port, limit=MAX_HEADER_BYTES
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(name, stop.set)
    print(
        f"live_proxy: listening on 127.0.0.1:{port}; hosts {sorted(hosts)}; suffixes {suffixes}",
        file=sys.stderr,
        flush=True,
    )
    async with server:
        await stop.wait()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--port", type=int, default=3128)
    args = parser.parse_args()
    asyncio.run(serve(args.manifest, args.port))


if __name__ == "__main__":
    main()
