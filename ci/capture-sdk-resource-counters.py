"""Capture public counters without assigning a termination cause to a PID."""

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import time


EVENT_KEYS = {"low", "high", "max", "oom", "oom_kill", "oom_group_kill"}
MEMINFO_KEYS = {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
CGROUP_COUNTERS = (
    "memory.events",
    "memory.events.local",
    "memory.max",
    "memory.peak",
    "memory.current",
    "memory.swap.max",
    "memory.swap.current",
)


def counter_file(path, keys=None):
    try:
        physical = path.stat()
        lines = path.read_text().splitlines()
        values = {}
        if keys is None:
            value = lines[0].strip()
            if value.isdecimal():
                values["value"] = int(value)
            elif value == "max":
                values["value"] = value
            else:
                raise ValueError("Noncanonical capacity value")
        else:
            for line in lines:
                parts = line.replace(":", "").split()
                if parts and parts[0] in keys:
                    if len(parts) < 2 or not parts[1].isdecimal():
                        raise ValueError("Noncanonical counter")
                    values[parts[0]] = int(parts[1])
        return {
            "available": True,
            "identity": [physical.st_dev, physical.st_ino],
            "values": values,
        }
    except (OSError, ValueError, IndexError) as error:
        return {"available": False, "reason": type(error).__name__}


def capture(phase):
    result = {
        "phase": phase,
        "timeNs": time.time_ns(),
        "observerPid": os.getpid(),
        "scope": (
            "Process cgroup counters can include other processes. "
            "No PID-specific termination cause inferred."
        ),
        "sources": {},
    }
    result["sources"]["meminfo"] = counter_file(Path("/proc/meminfo"), MEMINFO_KEYS)
    try:
        rows = [
            line.split(":", 2)
            for line in Path("/proc/self/cgroup").read_text().splitlines()
        ]
        paths = [
            row[2] for row in rows if len(row) == 3 and row[:2] == ["0", ""]
        ]
        if len(paths) != 1 or ".." in PurePosixPath(paths[0]).parts:
            raise ValueError("Unsupported cgroup scope")
        root = Path("/sys/fs/cgroup") / paths[0].lstrip("/")
        for name in CGROUP_COUNTERS:
            keys = EVENT_KEYS if name.startswith("memory.events") else None
            result["sources"][name] = counter_file(root / name, keys)
    except (OSError, ValueError) as error:
        result["sources"]["cgroup"] = {
            "available": False,
            "reason": type(error).__name__,
        }
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["before", "after"])
    parser.add_argument("--directory", required=True, type=Path)
    args = parser.parse_args()
    if args.directory.is_symlink():
        raise RuntimeError("Symlink diagnostic directory refused")
    if args.phase == "before":
        args.directory.mkdir(mode=0o700)
    if not args.directory.is_dir() or args.directory.stat().st_uid != os.getuid():
        raise RuntimeError("Foreign diagnostic directory")

    result = capture(args.phase)
    if args.phase == "after":
        before = json.loads((args.directory / "before.json").read_text())
        deltas = {}
        for name, current in result["sources"].items():
            prior = before["sources"].get(name, {})
            if (
                current.get("available")
                and prior.get("available")
                and current.get("identity") == prior.get("identity")
                and name.startswith("memory.events")
            ):
                deltas[name] = {
                    key: value - prior["values"][key]
                    for key, value in current["values"].items()
                    if key in prior["values"]
                }
        result["sameScopeEventDeltas"] = deltas

    with (args.directory / (args.phase + ".json")).open("x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
