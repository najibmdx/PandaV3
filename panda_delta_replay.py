#!/usr/bin/env python3
"""Replay runner for Panda delta emit engine (stdout + file)."""

import argparse
import sys
import csv
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from panda_delta_engine import DeltaEngine

SGT = ZoneInfo("Asia/Singapore")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def format_emit_lines(emit, explain):
    ts_str = datetime.fromtimestamp(emit["ts"], tz=SGT).strftime("%H:%M:%S")
    lines = [
        f"{ts_str}  {emit['dimension']} {emit['arrow']}",
        f"      {emit['primary']}",
        f"      {emit['context']}"
    ]
    if explain:
        lines.append(f"      Explanation: {emit['primary']}. {emit['context']}")
    if emit["scream"]:
        lines = [line.upper() for line in lines]
    return lines


def load_wallet_first_seen(path):
    first_seen = {}
    if not os.path.exists(path):
        return first_seen
    with open(path, "r") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("wallet\t"):
                continue
            wallet, ts_iso, _minute_iso = line.split("\t")
            ts_epoch = int(datetime.fromisoformat(ts_iso).timestamp())
            first_seen[wallet] = ts_epoch
    return first_seen


def iter_events_jsonl(path):
    with open(path, "r") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            ts = int(datetime.fromisoformat(obj["ts_iso"]).timestamp())
            yield {
                "ts": ts,
                "wallet": obj["wallet"],
                "side": obj["side"],
                "amount": obj["amount"],
                "is_new_wallet": bool(obj.get("is_new_wallet", False))
            }


def iter_events_csv(path, first_seen):
    with open(path, "r", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ts = int(row["ts"])
            wallet = row["wallet"]
            side = row["side"]
            amount = float(row["token_amt"])
            is_new_wallet = False
            if wallet in first_seen and abs(ts - first_seen[wallet]) <= 1:
                is_new_wallet = True
            yield {
                "ts": ts,
                "wallet": wallet,
                "side": side,
                "amount": amount,
                "is_new_wallet": is_new_wallet
            }


def main():
    parser = argparse.ArgumentParser(description="Panda delta replay")
    parser.add_argument("--mint", required=True)
    parser.add_argument("--in-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--explain", action="store_true")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    feed_path = os.path.join(args.out_dir, f"{args.mint}.delta_feed.txt")
    explain_path = os.path.join(args.out_dir, f"{args.mint}.delta_feed_replay_explain.txt")

    events_jsonl = os.path.join(args.in_dir, f"{args.mint}.events.jsonl")
    events_csv = os.path.join(args.in_dir, f"{args.mint}.events.csv")
    wallet_first_seen_path = os.path.join(args.in_dir, f"{args.mint}.wallet_first_seen.tsv")
    first_seen = load_wallet_first_seen(wallet_first_seen_path)

    if os.path.exists(events_jsonl):
        event_iter = iter_events_jsonl(events_jsonl)
    elif os.path.exists(events_csv):
        event_iter = iter_events_csv(events_csv, first_seen)
    else:
        raise SystemExit("No events.jsonl or events.csv found")

    engine = DeltaEngine()

    with open(feed_path, "w", encoding="utf-8", newline="\n") as feed_handle:
        explain_handle = None
        if args.explain:
            explain_handle = open(explain_path, "w", encoding="utf-8", newline="\n")
        try:
            for event in event_iter:
                emits = engine.on_event(event, explain=args.explain)
                for emit in emits:
                    lines = format_emit_lines(emit, args.explain)
                    for line in lines:
                        print(line)
                        feed_handle.write(line + "\n")
                        if explain_handle:
                            explain_handle.write(line + "\n")
        finally:
            if explain_handle:
                explain_handle.close()


if __name__ == "__main__":
    main()
