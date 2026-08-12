#!/usr/bin/env python3
import json, os, subprocess, sys

ENABLED = {
    "folder": True,
    "branch": True,
    "model": True,
    "context": True,
    "cost": False,
    "rate_limits": False,
}


def main():
    data = {}
    try:
        raw = sys.stdin.read()
        if raw.strip():
            data = json.loads(raw)
    except Exception:
        pass

    parts = []

    if ENABLED["folder"]:
        cwd = data.get("cwd") or os.getcwd()
        parts.append(os.path.basename(cwd) or cwd)

    if ENABLED["branch"]:
        branch = (data.get("worktree") or {}).get("branch") or ""
        if not branch:
            try:
                r = subprocess.run(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
                branch = r.stdout.strip() if r.returncode == 0 else ""
            except Exception:
                pass
        parts.append(branch or "no-branch")

    if ENABLED["model"]:
        m = data.get("model") or {}
        label = m.get("display_name") or m.get("id") or "unknown"
        parts.append(label)

    if ENABLED["context"]:
        cw = data.get("context_window") or {}
        used_pct = cw.get("used_percentage")
        rem_pct = cw.get("remaining_percentage")
        if used_pct is None:
            parts.append("ctx:[????????????????????] awaiting data")
        else:
            u = int(round(used_pct))
            filled = u * 20 // 100
            bar = "#" * filled + "-" * (20 - filled)
            r = int(round(rem_pct)) if rem_pct is not None else (100 - u)
            parts.append(f"ctx:[{bar}] {u}% used ({r}% left)")

    if ENABLED["cost"]:
        cost = (data.get("cost") or {}).get("total_cost_usd")
        if cost is None:
            cost = 0.0
        parts.append(f"${cost:.2f}")

    if ENABLED["rate_limits"]:
        rl = data.get("rate_limits") or {}
        segs = []
        for key, label in [("five_hour", "5h"), ("seven_day", "7d")]:
            pct = (rl.get(key) or {}).get("used_percentage")
            if pct is not None:
                segs.append(f"{label}:{pct:.0f}%")
        if segs:
            parts.append(" ".join(segs))

    if parts:
        print(" | ".join(parts))


if __name__ == "__main__":
    main()
