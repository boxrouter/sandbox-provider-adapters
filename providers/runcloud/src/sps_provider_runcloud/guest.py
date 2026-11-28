# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""The supervisor that `run` and the process operations start inside a sandbox.

run.cloud's exec call holds one HTTP request open for the whole command and returns its output as
JSON text. That cannot carry a command longer than the edge allows, output that is not UTF-8, or a
background process. So the adapter never execs the caller's argv directly. It execs `python3 -c
SUPERVISOR` instead, which starts the argv as a detached process in its own session, writes its
output and exit status to files under `/tmp/.sps/`, and answers with a small JSON document. Later
execs of the same script in `poll` mode wait for new output or the exit, and return bytes from
given offsets as base64. The files stay until the sandbox ends (`run` removes its own).

`start` argv: start <dir> <spec JSON> <wait seconds> <max bytes per stream>; the spec holds the
              argv, cwd, env, timeout, the read caps and whether to remove a finished record
`poll`  argv: poll <dir> <wait seconds> <out offset> <err offset> <out cap> <err cap>
              <max bytes per stream> <remove when read: 0 or 1>

The script prints one JSON object on stdout: `{"error": "cwd" | "exists" | "gone"}`, or the state
`{"done": bool, "exit": {"code", "timed_out"} | null, "out", "err" (base64), "out_size",
"err_size"}`. The script is passed as an argument, so nothing is written into the sandbox first.
"""

ROOT = "/tmp/.sps"

SUPERVISOR = r"""
import base64, json, os, signal, subprocess, sys, time

def size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0

def chunk(path, offset, cap, most):
    end = min(size(path), cap)
    if end <= offset:
        return b""
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(min(end - offset, most))

def exit_doc(d):
    try:
        with open(d + "/exit") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None

def state(d, wait, oo, eo, ocap, ecap, most):
    deadline = time.monotonic() + wait
    pause = 0.02
    while True:
        done = exit_doc(d)
        if done is not None or size(d + "/out") > oo or size(d + "/err") > eo:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(min(pause, max(deadline - time.monotonic(), 0)))
        pause = min(pause * 2, 0.25)
    out = chunk(d + "/out", oo, ocap, most)
    err = chunk(d + "/err", eo, ecap, most)
    return {
        "done": done is not None,
        "exit": done,
        "out": base64.b64encode(out).decode(),
        "err": base64.b64encode(err).decode(),
        "out_size": size(d + "/out"),
        "err_size": size(d + "/err"),
    }

def session_members(sid):
    found = []
    try:
        names = os.listdir("/proc")
    except OSError:
        return found
    for name in names:
        if not name.isdigit():
            continue
        try:
            with open("/proc/%s/stat" % name) as f:
                fields = f.read().rsplit(")", 1)[1].split()
        except OSError:
            continue
        if int(fields[3]) == sid:
            found.append(int(name))
    return found

def end_session(sid):
    for _ in range(20):
        try:
            os.killpg(sid, signal.SIGKILL)
        except OSError:
            pass
        members = session_members(sid)
        if not members:
            return
        for pid in members:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        time.sleep(0.05)

def write_exit(d, doc):
    with open(d + "/exit.tmp", "w") as f:
        json.dump(doc, f)
    os.rename(d + "/exit.tmp", d + "/exit")

def supervise(d, spec, ready):
    devnull = os.open(os.devnull, os.O_RDWR)
    for fd in (0, 1, 2):
        os.dup2(devnull, fd)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    out = os.open(d + "/out", flags, 0o600)
    err = os.open(d + "/err", flags, 0o600)
    env = dict(os.environ)
    env.update(spec["env"])
    try:
        child = subprocess.Popen(
            spec["argv"], cwd=spec["cwd"], env=env, stdin=devnull, stdout=out, stderr=err,
            start_new_session=True, close_fds=True,
        )
    except OSError as error:
        os.write(err, ("%s: %s\n" % (spec["argv"][0], error.strerror)).encode())
        code = 127 if isinstance(error, FileNotFoundError) else 126
        write_exit(d, {"code": code, "timed_out": False})
        os.write(ready, b"x")
        return
    os.write(ready, b"x")
    os.close(ready)
    timed_out = False
    try:
        code = child.wait(spec["timeout"])
    except subprocess.TimeoutExpired:
        timed_out = True
        end_session(child.pid)
        code = child.wait()
    write_exit(d, {"code": 128 - code if code < 0 else code, "timed_out": timed_out})

def finish(d, result, oo, eo, ocap, ecap, remove):
    read_all = (
        oo + len(base64.b64decode(result["out"])) >= min(result["out_size"], ocap)
        and eo + len(base64.b64decode(result["err"])) >= min(result["err_size"], ecap)
    )
    if remove and result["done"] and read_all:
        for name in ("out", "err", "exit"):
            try:
                os.unlink(d + "/" + name)
            except OSError:
                pass
        try:
            os.rmdir(d)
        except OSError:
            pass
    return result

def start(d, spec, wait, most):
    if spec["cwd"] is not None and not os.path.isdir(spec["cwd"]):
        return {"error": "cwd"}
    os.makedirs(os.path.dirname(d), exist_ok=True)
    try:
        os.mkdir(d, 0o700)
    except FileExistsError:
        return {"error": "exists"}
    ready_r, ready_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(ready_r)
        os.setsid()
        if os.fork() == 0:
            try:
                supervise(d, spec, ready_w)
            finally:
                os._exit(0)
        os._exit(0)
    os.close(ready_w)
    os.read(ready_r, 1)
    os.close(ready_r)
    os.waitpid(pid, 0)
    ocap, ecap = spec["caps"]
    result = state(d, wait, 0, 0, ocap, ecap, most)
    return finish(d, result, 0, 0, ocap, ecap, spec["remove"])

def main(argv):
    if argv[0] == "start":
        return start(argv[1], json.loads(argv[2]), float(argv[3]), int(argv[4]))
    d = argv[1]
    if not os.path.isdir(d):
        return {"error": "gone"}
    oo, eo, ocap, ecap, most = (int(v) for v in argv[3:8])
    result = state(d, float(argv[2]), oo, eo, ocap, ecap, most)
    return finish(d, result, oo, eo, ocap, ecap, argv[8] == "1")

sys.stdout.write(json.dumps(main(sys.argv[1:])))
"""
