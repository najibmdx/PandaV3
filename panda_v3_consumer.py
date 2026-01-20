#!/usr/bin/env python3
"""Step 9 V3 evidence consumer (read-only)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

class Mode(str, Enum):
    STRICT = "STRICT"
    PERMISSIVE = "PERMISSIVE"
    AUDIT = "AUDIT"


class TrustLevel(str, Enum):
    RAW = "RAW"
    VALIDATED_IN_RUN = "VALIDATED_IN_RUN"


V3_EVIDENCE_REQUIRED_KEYS = [
    "ts_iso",
    "trigger",
    "active",
    "confidence",
    "subject_type",
    "subject_id",
    "window_seconds",
    "metrics",
    "reason",
]


ALERTS_HEADER = [
    "ts_iso",
    "mint",
    "emit_type",
    "category",
    "confidence",
    "subject_type",
    "subject_id",
    "subject_members",
    "trigger_type",
    "trigger_id",
    "corr_ids",
    "intel",
    "warning",
    "evidence_status",
    "evidence_summary",
]


@dataclass
class ErrorEntry:
    record_id: str
    reason: str

    def to_dict(self) -> Dict[str, str]:
        return {"record_id": self.record_id, "reason": self.reason}


@dataclass
class Counters:
    evidence_total_lines: int = 0
    evidence_total_records: int = 0
    evidence_valid_records: int = 0
    evidence_invalid_records: int = 0
    alerts_total_lines: int = 0
    alerts_total_records: int = 0
    alerts_valid_records: int = 0
    alerts_invalid_records: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "evidence_total_lines": self.evidence_total_lines,
            "evidence_total_records": self.evidence_total_records,
            "evidence_valid_records": self.evidence_valid_records,
            "evidence_invalid_records": self.evidence_invalid_records,
            "alerts_total_lines": self.alerts_total_lines,
            "alerts_total_records": self.alerts_total_records,
            "alerts_valid_records": self.alerts_valid_records,
            "alerts_invalid_records": self.alerts_invalid_records,
        }


def record_id(path: str, line_number: int) -> str:
    return f"{os.path.basename(path)}:{line_number}"


def emit_audit_record(handle, payload: Dict[str, Any]) -> None:
    handle.write(json.dumps(payload) + "\n")


def handle_error(
    mode: Mode,
    counters: Counters,
    errors: List[ErrorEntry],
    max_errors: int,
    record_id_value: str,
    reason: str,
    audit_handle=None,
    audit_payload: Optional[Dict[str, Any]] = None,
    increment_invalid: Optional[str] = None,
) -> None:
    if increment_invalid == "evidence":
        counters.evidence_invalid_records += 1
    elif increment_invalid == "alerts":
        counters.alerts_invalid_records += 1

    if mode == Mode.STRICT:
        raise SystemExit(f"{record_id_value} {reason}")

    if len(errors) < max_errors:
        errors.append(ErrorEntry(record_id_value, reason))

    if mode == Mode.AUDIT and audit_handle and audit_payload:
        emit_audit_record(audit_handle, audit_payload)


def validate_evidence_record(obj: Dict[str, Any]) -> Optional[str]:
    for key in V3_EVIDENCE_REQUIRED_KEYS:
        if key not in obj:
            return f"missing_required_key:{key}"
    metrics = obj.get("metrics")
    if not isinstance(metrics, dict):
        return "metrics_not_object"
    return None


def process_evidence(
    path: str,
    mode: Mode,
    trust_level: TrustLevel,
    counters: Counters,
    max_errors: int,
    errors: List[ErrorEntry],
    audit_handle,
    trigger_counts: Counter,
) -> None:
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            counters.evidence_total_lines += 1
            raw_line = line.rstrip("\n")
            if not raw_line.strip():
                continue
            counters.evidence_total_records += 1
            rec_id = record_id(path, line_number)
            try:
                obj = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                handle_error(
                    mode,
                    counters,
                    errors,
                    max_errors,
                    rec_id,
                    f"invalid_json:{exc.msg}",
                    audit_handle=audit_handle,
                    audit_payload={
                        "_valid": False,
                        "_error": f"invalid_json:{exc.msg}",
                        "_record_id": rec_id,
                        "source": "evidence",
                        "raw": raw_line,
                    },
                    increment_invalid="evidence",
                )
                continue

            if trust_level == TrustLevel.VALIDATED_IN_RUN:
                validation_error = validate_evidence_record(obj)
                if validation_error:
                    handle_error(
                        mode,
                        counters,
                        errors,
                        max_errors,
                        rec_id,
                        validation_error,
                        audit_handle=audit_handle,
                        audit_payload={
                            "_valid": False,
                            "_error": validation_error,
                            "_record_id": rec_id,
                            "source": "evidence",
                            "record": obj,
                        },
                        increment_invalid="evidence",
                    )
                    continue

            counters.evidence_valid_records += 1
            trigger = obj.get("trigger")
            if isinstance(trigger, str) and trigger:
                trigger_counts[trigger] += 1


def parse_alerts_header(
    path: str,
    line: str,
    line_number: int,
    mode: Mode,
    counters: Counters,
    max_errors: int,
    errors: List[ErrorEntry],
    audit_handle,
) -> bool:
    rec_id = record_id(path, line_number)
    raw_line = line.rstrip("\n")
    if not raw_line.strip():
        return True
    columns = raw_line.split("\t")
    if columns != ALERTS_HEADER:
        reason = "header_mismatch"
        handle_error(
            mode,
            counters,
            errors,
            max_errors,
            rec_id,
            reason,
            audit_handle=audit_handle,
            audit_payload={
                "_valid": False,
                "_error": reason,
                "_record_id": rec_id,
                "source": "alerts",
                "raw": raw_line,
            },
            increment_invalid="alerts",
        )
        return mode != Mode.STRICT
    return True


def process_alerts(
    path: str,
    mode: Mode,
    counters: Counters,
    max_errors: int,
    errors: List[ErrorEntry],
    audit_handle,
    emit_type_counts: Counter,
    category_counts: Counter,
    trigger_counts: Counter,
) -> None:
    with open(path, "r", encoding="utf-8") as handle:
        header_checked = False
        for line_number, line in enumerate(handle, start=1):
            counters.alerts_total_lines += 1
            if not header_checked:
                header_checked = True
                parse_alerts_header(
                    path,
                    line,
                    line_number,
                    mode,
                    counters,
                    max_errors,
                    errors,
                    audit_handle,
                )
                continue

            rec_id = record_id(path, line_number)
            row = line.rstrip("\n")
            if not row.strip():
                continue
            counters.alerts_total_records += 1
            columns = row.split("\t")
            if len(columns) != len(ALERTS_HEADER):
                reason = f"column_count_mismatch:{len(columns)}"
                handle_error(
                    mode,
                    counters,
                    errors,
                    max_errors,
                    rec_id,
                    reason,
                    audit_handle=audit_handle,
                    audit_payload={
                        "_valid": False,
                        "_error": reason,
                        "_record_id": rec_id,
                        "source": "alerts",
                        "raw": row,
                    },
                    increment_invalid="alerts",
                )
                continue

            counters.alerts_valid_records += 1
            emit_type = columns[ALERTS_HEADER.index("emit_type")]
            category = columns[ALERTS_HEADER.index("category")]
            trigger_type = columns[ALERTS_HEADER.index("trigger_type")]
            if emit_type:
                emit_type_counts[emit_type] += 1
            if category:
                category_counts[category] += 1
            if trigger_type:
                trigger_counts[trigger_type] += 1


def print_counts(title: str, counts: Counter) -> None:
    print(title)
    if not counts:
        print("  (none)")
        return
    for key in sorted(counts):
        print(f"  {key}: {counts[key]}")


def write_report(
    path: str,
    mode: Mode,
    trust_level: TrustLevel,
    evidence_path: str,
    alerts_path: str,
    counters: Counters,
    errors: List[ErrorEntry],
    trigger_counts: Counter,
    emit_type_counts: Counter,
    category_counts: Counter,
) -> None:
    report = {
        "mode": mode.value,
        "trust_level": trust_level.value,
        "inputs": {"evidence": evidence_path, "alerts": alerts_path},
        "counters": counters.to_dict(),
        "errors": [entry.to_dict() for entry in errors],
        "counts": {
            "trigger": dict(trigger_counts),
            "emit_type": dict(emit_type_counts),
            "category": dict(category_counts),
        },
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 9 V3 evidence consumer")
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--alerts", required=True)
    parser.add_argument("--mode", choices=[m.value for m in Mode], default=Mode.STRICT.value)
    parser.add_argument("--validate-in-run", type=int, choices=[0, 1], default=0)
    parser.add_argument("--max-errors", type=int, default=5)
    parser.add_argument("--report-out")
    parser.add_argument("--audit-out")
    args = parser.parse_args()

    mode = Mode(args.mode)
    trust_level = TrustLevel.VALIDATED_IN_RUN if args.validate_in_run == 1 else TrustLevel.RAW
    counters = Counters()
    errors: List[ErrorEntry] = []
    trigger_counts: Counter = Counter()
    emit_type_counts: Counter = Counter()
    category_counts: Counter = Counter()

    audit_handle = None
    if args.audit_out:
        if mode != Mode.AUDIT:
            raise SystemExit("--audit-out requires --mode AUDIT")
        audit_handle = open(args.audit_out, "w", encoding="utf-8")

    try:
        process_evidence(
            args.evidence,
            mode,
            trust_level,
            counters,
            args.max_errors,
            errors,
            audit_handle,
            trigger_counts,
        )
        process_alerts(
            args.alerts,
            mode,
            counters,
            args.max_errors,
            errors,
            audit_handle,
            emit_type_counts,
            category_counts,
            trigger_counts,
        )
    finally:
        if audit_handle:
            audit_handle.close()

    print("V3 Evidence Consumer Summary")
    print(f"Mode: {mode.value}")
    print(f"Trust level: {trust_level.value}")
    print(f"Evidence input: {args.evidence}")
    print(f"Alerts input: {args.alerts}")
    print("Counters:")
    for key, value in counters.to_dict().items():
        print(f"  {key}: {value}")
    print_counts("Trigger counts:", trigger_counts)
    print_counts("Emit type counts:", emit_type_counts)
    print_counts("Category counts:", category_counts)

    if errors:
        print("Errors:")
        for entry in errors:
            print(f"  {entry.record_id}: {entry.reason}")

    if args.report_out:
        write_report(
            args.report_out,
            mode,
            trust_level,
            args.evidence,
            args.alerts,
            counters,
            errors,
            trigger_counts,
            emit_type_counts,
            category_counts,
        )


if __name__ == "__main__":
    main()
