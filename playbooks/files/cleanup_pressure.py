"""Guest-side cleanup for one demo VM. Run as rheluser, not as root."""

import json
import os
from pathlib import Path
import pwd
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time

THRESHOLD = 80
MIN_SHARE = 10  # percentage of host CPU or physical RAM, not percentage of one core


def ctl(user, *args, stop_transient=False):
    cmd = (["sudo", "-n"] if stop_transient else []) + ["systemctl"]
    if user:
        cmd.append("--user")
    return subprocess.check_output(cmd + list(args), text=True, stderr=subprocess.PIPE, timeout=30).strip()


def host_sample():
    cpu = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:9]))
    mem = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    total = int(mem["MemTotal"].split()[0]) * 1024
    free = int(mem["MemAvailable"].split()[0]) * 1024
    return sum(cpu), cpu[3] + cpu[4], total, 100 * (1 - free / total)


def user_allowlist():
    path = Path("/etc/aiops/removable-user-services")
    if not path.exists():
        return set()
    if (path.resolve() != path or not stat.S_ISREG(path.stat().st_mode)
            or any(p.stat().st_uid != 0 or p.stat().st_mode & 0o022 for p in (path, path.parent))):
        raise ValueError("unsafe removable-user-services configuration")
    return {line.strip() for line in path.read_text().splitlines()
            if re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*\.service", line.strip())}


def eligible(vm, user, name, allowed):
    if name in ("aiops-demo.service", "sshd.service", "NetworkManager.service"):
        return False
    if user:
        return bool(name in allowed or re.fullmatch(r"aiops-disposable-[a-zA-Z0-9_-]+\.service", name))
    return bool(re.fullmatch(rf"chaos-stress-{re.escape(vm)}-[0-9]+\.service", name))


def unit_info(vm, user, name, allowed):
    if not eligible(vm, user, name, allowed):
        return None
    props = "Id,ActiveState,ControlGroup,InvocationID,FragmentPath,Transient,RequiredBy,BoundBy,TriggeredBy"
    data = dict(line.split("=", 1) for line in ctl(user, "show", name, "--property", props).splitlines())
    if (data["Id"] != name or data["ActiveState"] != "active" or not data["InvocationID"]
            or any(data[k] for k in ("RequiredBy", "BoundBy", "TriggeredBy"))):
        return None
    unit_file = Path(data["FragmentPath"])
    if user:
        if (unit_file != Path.home() / ".config/systemd/user" / name or not unit_file.is_file()
                or unit_file.resolve() != unit_file or unit_file.stat().st_uid != os.getuid()):
            return None
    elif data["Transient"] != "yes" or unit_file != Path("/run/systemd/transient") / name:
        return None
    group = Path(data["ControlGroup"])
    if not group.is_absolute() or group.name != name or ".." in group.parts:
        return None
    cg = Path("/sys/fs/cgroup") / group.relative_to("/")
    return data["InvocationID"], unit_file, cg


def main(vm):
    if not re.fullmatch(r"rhel10-service-[1-4]", vm) or pwd.getpwuid(os.getuid()).pw_name != "rheluser":
        raise ValueError("requires the demo VM and its unprivileged demo account")
    allowed = user_allowlist()
    candidates = []
    for user, glob in [(True, "*.service"), (False, f"chaos-stress-{vm}-*.service")]:
        for line in ctl(user, "list-units", "--state=active", "--no-legend", "--plain", glob).splitlines():
            name = line.split()[0]
            info = unit_info(vm, user, name, allowed)
            if info:
                try:
                    candidates.append((user, name, info, cpu_usec(info[2])))
                except FileNotFoundError:
                    pass  # transient service finished during discovery
    first = host_sample()
    start = time.monotonic()
    time.sleep(2)
    ticks, idle, total, memory = host_sample()
    cpu = 100 * (1 - (idle - first[1]) / max(1, ticks - first[0]))
    elapsed = time.monotonic() - start
    result = {"cpu_pct": round(cpu, 1), "memory_pct": round(memory, 1), "removed": None}
    if max(cpu, memory) <= THRESHOLD:
        return result
    ranked = []
    for user, name, info, previous in candidates:
        try:
            cpu_share = (cpu_usec(info[2]) - previous) / (elapsed * 10000 * os.cpu_count())
            ram_share = 100 * int((info[2] / "memory.current").read_text()) / total
        except FileNotFoundError:
            continue
        impact = max(cpu_share if cpu > THRESHOLD else 0, ram_share if memory > THRESHOLD else 0)
        if impact >= MIN_SHARE:
            ranked.append((impact, user, name, info, cpu_share, ram_share))
    if not ranked:
        raise RuntimeError("pressure persists but no removable service is a significant contributor")
    _, user, name, info, cpu_share, ram_share = max(ranked)
    if info != unit_info(vm, user, name, user_allowlist()):
        raise RuntimeError("candidate changed during measurement")
    result.update(unit_cpu_pct=round(cpu_share, 1), unit_memory_pct=round(ram_share, 1),
                  pids=[int(p) for p in (info[2] / "cgroup.procs").read_text().split()][:16])
    if user:
        backup_dir = Path.home() / ".local/state/aiops-recovery"
        backup_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        backup = Path(tempfile.mkdtemp(dir=backup_dir)) / name
        shutil.copy2(info[1], backup)
        ctl(True, "disable", "--now", name)
        info[1].unlink()
        ctl(True, "daemon-reload")
        result["backup"] = str(backup)
    else:
        # systemd removes the transient unit when it stops. Never unlink a system unit.
        ctl(False, "stop", name, stop_transient=True)
    result["removed"] = name
    return result


def cpu_usec(group):
    return int(dict(line.split() for line in (group / "cpu.stat").read_text().splitlines())["usage_usec"])


if __name__ == "__main__":
    print(json.dumps(main(sys.argv[1])))
