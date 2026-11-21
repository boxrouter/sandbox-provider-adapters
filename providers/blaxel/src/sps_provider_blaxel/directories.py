# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""The four directory operations, as the commands that the spec defines them by.

Blaxel's own file API cannot give these results: its `HEAD` follows a link and answers 200 for a
missing path, and its listing has no size for a directory. Section 18 of the contract says each
result is the one that the equivalent commands give, so each operation runs one short program in
the sandbox as the default user: a POSIX script (`mkdir`, `rm`, `test`) or, for the listing, a few
lines of Python. A program ends with a code of its own when the path is missing (40) or is not a
directory (41); any other failure is told by the command's message, which the scripts make English
with `LC_ALL=C`.
"""

from __future__ import annotations

import base64
import binascii

from sandbox_provider_spec.v1 import DirectoryEntry, DirectoryPage, EntryType, RunResult, SpsError

MISSING = 40
NOT_A_DIRECTORY = 41

_LC = "# sps-directory\nLC_ALL=C; export LC_ALL\n"
# The listing is a Python program (the base image has python3, and its BusyBox `find` has no
# `-printf`): one `lstat` per entry, one NUL-ended `type<TAB>size<TAB>name` record each.
LIST = """# sps-directory
import os, stat, sys
path = sys.argv[1]
if not os.path.lexists(path):
    sys.exit(40)
if not os.path.isdir(path):
    sys.exit(41)
out = sys.stdout.buffer
try:
    for entry in os.scandir(path):
        info = entry.stat(follow_symlinks=False)
        mode = info.st_mode
        kind = "o"
        if stat.S_ISDIR(mode):
            kind = "d"
        elif stat.S_ISREG(mode):
            kind = "f"
        elif stat.S_ISLNK(mode):
            kind = "l"
        out.write(f"{kind}\\t{info.st_size}\\t".encode() + os.fsencode(entry.name) + b"\\0")
except PermissionError:
    sys.stderr.write("Permission denied")
    sys.exit(1)
"""
# `-L` and `-d` follow a link, so the link is asked first: a link is a link, even a dangling one.
EXISTS = (
    _LC
    + '[ -L "$1" ] && { printf l; exit 0; }\n'
    + '[ -d "$1" ] && { printf d; exit 0; }\n'
    + '[ -f "$1" ] && { printf f; exit 0; }\n'
    + '[ -e "$1" ] && { printf o; exit 0; }\n'
    + "exit 40\n"
)
MKDIR = _LC + 'if [ "$2" = 1 ]; then mkdir -p -- "$1"; else [ -d "$1" ] || mkdir -- "$1"; fi\n'
REMOVE = (
    _LC
    + '[ -e "$1" ] || [ -L "$1" ] || exit 40\n'
    + 'if [ -d "$1" ] && [ ! -L "$1" ]; then\n'
    + '  if [ "$2" = 1 ]; then rm -r -- "$1"; else rmdir -- "$1"; fi\n'
    + 'else rm -- "$1"; fi\n'
)

_TYPES: dict[str, EntryType] = {"f": "file", "d": "directory", "l": "symlink"}


def argv(script: str, path: str, flag: bool = False) -> list[str]:
    """The command line of a script; a relative path is anchored so that it is never an option."""
    where = path if path.startswith("/") else f"./{path}"
    if script is LIST:
        return ["python3", "-c", script, where]
    return ["sh", "-c", script, "sh", where, "1" if flag else "0"]


def check_path(path: str) -> None:
    if not path:
        raise SpsError("invalid_request", "The path is empty")


def entry_type(code: str) -> EntryType:
    """`find`'s `%y` letter as a contract type; a FIFO, socket or device is `other`."""
    return _TYPES.get(code, "other")


def failure(result: RunResult, what: str) -> SpsError:
    """The `SpsError` for a script that failed. Messages never carry the command's output."""
    code, said = result.exit_code, result.stderr
    if code == MISSING:
        return SpsError("not_found", f"{what}: no such path")
    if code == NOT_A_DIRECTORY:
        return SpsError("invalid_request", f"{what}: not a directory")
    if code == 127:
        return SpsError("unsupported", f"{what}: the sandbox's image lacks a command it needs")
    if "Permission denied" in said or "Operation not permitted" in said:
        return SpsError("forbidden", f"{what}: permission denied")
    if "File exists" in said or "Not a directory" in said or "not empty" in said:
        return SpsError("conflict", f"{what}: the path is in the way")
    if "No such file or directory" in said:
        return SpsError("not_found", f"{what}: no such path")
    # An unexplained failure may have gone part way (a recursive removal).
    return SpsError("provider_error", f"{what} failed", outcome="unknown")


def parse_listing(stdout: str) -> list[DirectoryEntry]:
    """The entries of `LIST`'s output (`type<TAB>size<TAB>name`, each ended by NUL), by name bytes."""
    found: list[DirectoryEntry] = []
    for record in filter(None, stdout.split("\0")):
        code, size, name = record.split("\t", 2)
        found.append(DirectoryEntry(name=name, type=entry_type(code), size=int(size)))
    return sorted(found, key=lambda e: e.name.encode())


def page(
    entries: list[DirectoryEntry], cursor: str | None, limit: int, maximum: int
) -> DirectoryPage:
    """One page after `cursor`, which is the last name of the page before, in name order."""
    if not 1 <= limit <= maximum:
        raise SpsError("invalid_request", f"limit must be 1 to {maximum}")
    after: bytes | None = None
    if cursor:
        try:
            after = base64.b64decode(cursor.encode(), altchars=b"-_", validate=True)
        except (binascii.Error, ValueError):
            raise SpsError("invalid_request", "The cursor is not one this adapter gave") from None
    rest = [e for e in entries if after is None or e.name.encode() > after]
    shown = rest[:limit]
    more = len(rest) > limit
    token = base64.urlsafe_b64encode(shown[-1].name.encode()).decode() if more else None
    return DirectoryPage(entries=shown, next_cursor=token)
