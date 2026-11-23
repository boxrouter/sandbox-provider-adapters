# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""The four directory operations, as the commands that the spec defines them by.

Section 18 of the contract says each result is the one that the equivalent commands give. Blaxel's
own file API cannot give `mkdir`, `rm` and `exists` (its `HEAD` follows a link and answers 200 for a
missing path), so each runs one short POSIX script (`mkdir`, `rm`, `test`) in the sandbox as the
default user. A script ends with a code of its own when the path is missing (40) or is not a
directory (41); any other failure is told by the command's message, which the scripts make English
with `LC_ALL=C`.

The listing is Blaxel's own `GET /filesystem/{dir}` (one exchange): it names every entry and gives
a file's size and its mode as an octal Go `FileMode` string, but gives a directory no size.
`listing` reads the entry type from the mode's type bits and `DIR_SIZES` asks `stat` for the size
of the directories on the page being returned, in one process, only when that page has a
directory.
"""

from __future__ import annotations

import base64
import binascii
import re

from sandbox_provider_spec.v1 import DirectoryEntry, DirectoryPage, EntryType, RunResult, SpsError

MISSING = 40
NOT_A_DIRECTORY = 41

_LC = "# sps-directory\nLC_ALL=C; export LC_ALL\n"
# The size of each directory on a page, in the order given: `stat` does not follow a link.
DIR_SIZES = ["stat", "-c", "%s", "--"]
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


# Go's `FileMode` printed in octal, which is what `permissions` carries: the type lives in the
# bits above the nine permission bits (a link "1000000777" is symlink 1<<27 | 0777, a pipe
# "200000644" is pipe 1<<25 | 0644). Setuid, setgid and sticky are mode bits, not a type.
_MODE_DIR = 1 << 31
_MODE_SYMLINK = 1 << 27
# device, named pipe, socket, char device, irregular: the modes of a `files` entry that is not a
# regular file or a link.
_MODE_OTHER = (1 << 26) | (1 << 25) | (1 << 24) | (1 << 21) | (1 << 19)
_OCTAL = re.compile(r"[0-7]{1,16}")


def _kind(mode: object) -> EntryType | None:
    """The type in a `permissions` octal (`"644"` file, `"1000000777"` link, `"200000644"` pipe);
    None when the value does not read as a Go `FileMode`."""
    if not isinstance(mode, str) or not _OCTAL.fullmatch(mode):
        return None
    bits = int(mode, 8)
    if bits & _MODE_DIR:
        return "directory"
    if bits & _MODE_SYMLINK:
        return "symlink"
    return "other" if bits & _MODE_OTHER else "file"


def listing(body: object) -> list[DirectoryEntry]:
    """The entries of Blaxel's directory reply, by name bytes. A directory's size is 0 here: the
    caller asks for the directories it returns. An entry without a name, a size or a readable
    mode is a `provider_error`: the listing cannot be trusted."""
    if not isinstance(body, dict) or not ("files" in body or "subdirectories" in body):
        raise SpsError("invalid_request", "the listing: not a directory")
    found: list[DirectoryEntry] = []
    for item in body.get("files") or []:
        name = item.get("name") if isinstance(item, dict) else None
        size = item.get("size") if isinstance(item, dict) else None
        kind = _kind(item.get("permissions") if isinstance(item, dict) else None)
        if not isinstance(name, str) or not name or not isinstance(size, int) or kind is None:
            raise SpsError("provider_error", "the listing: an entry did not read")
        found.append(DirectoryEntry(name=name, type=kind, size=size))
    for item in body.get("subdirectories") or []:
        name = item.get("name") if isinstance(item, dict) else None
        if not isinstance(name, str) or not name:
            raise SpsError("provider_error", "the listing: an entry did not read")
        found.append(DirectoryEntry(name=name, type="directory", size=0))
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
