#!/usr/bin/env python3
"""
PANDA - Wallet Intelligence Scanner (Stateless, Live Forward)
Spec-locked implementation for Duck × Goose × Panda system

CRITICAL: Panda is a LIVE FORWARD SCANNER, not a historical backfill tool.
- Attaches to mint at launch time
- Polls for NEW transactions as they happen
- Processes events in time order (forward)
- No history reconstruction
- If stopped, restart to resume stream
"""

import argparse
import sys
import json
import os
import sys
import time
import signal
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict, deque
from panda_d import PandaD
from panda_delta_engine import DeltaEngine
from pandascan_u_p2 import build_phase2_contexts
import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# SGT timezone (UTC+8)
SGT = ZoneInfo("Asia/Singapore")

# FROZEN: Live stream configuration
POLL_INTERVAL_SECONDS = 2
MAX_CONSECUTIVE_EMPTY_POLLS = 10  # legacy; scanner must not exit on idle

# Signal constants from spec
K_MINUTES = 5  # Recent activity buffer
B_MINUTES = 15  # Baseline buffer
MIN_BUCKETS = 2  # Minimum data before signals
EVENT_COOLDOWN = 60  # Standard event cooldown (seconds)
WHALE_COOLDOWN = 30  # Whale event cooldown (seconds)
EPS = 1e-9  # Small epsilon for division safety
LEGACY_EMIT_INTERVAL_SEC = 0.6


class PandaScanner:
    """Stateless wallet intelligence scanner for a single mint"""
    
    def __init__(self, mint, outdir, helius_key, delta_only=False):
        self.mint = mint
        self.outdir = outdir
        self.helius_key = helius_key
        self.delta_only = delta_only
        self.helius_url = f"https://api.helius.xyz/v0/addresses/{mint}/transactions"
        self.attach_epoch = None
        self.attach_ts = None
        
        # In-memory state (per-run only)
        self.seen_sigs = set()
        self.wallet_first_seen = {}  # wallet -> (first_seen_ts_iso, first_seen_minute_iso)
        self.wallet_has_bought = set()  # wallets that have bought in session
        self.wallet_has_sold = set()  # wallets that have sold in session
        
        # Minute bar tracking (ordered dict to maintain time order)
        self.minute_buckets = {}  # minute_ts -> bucket_data
        self.completed_minutes = []  # sorted list of completed minute timestamps
        
        # Signal state tracking (STATE signals only)
        self.signal_state = {
            'expansion_under_pressure': False,
            'replacement_dominance': False,
            'broadening_control': False,
            'last_ignition_ts': 0,
            'last_fatigue_ts': 0,
            'last_whale_buy_ts': 0
        }
        
        # File handles
        self.events_file = None
        self.alerts_file = None
        self.v3_alerts_file = None
        self.minutes_file = None
        self.events_jsonl_file = None
        self.v3_evidence_file = None
        self.minutes_jsonl_file = None
        self.delta_feed_file = None
        self.alerts_emitted = []
        self.panda_d = PandaD()
        self.panda_d_outputs = {}
        self.rt_last_minute_emitted = None
        self.rt_last_tick = 0
        self.delta_engine = DeltaEngine()
        self.legacy_outbox = deque()
        self.legacy_next_emit_ts = 0.0
        self._stop = False
        
    def init_files(self, fresh):
        """Initialize output files with headers"""
        self.fresh = fresh
        events_path = os.path.join(self.outdir, f"{self.mint}.events.csv")
        alerts_path = os.path.join(self.outdir, f"{self.mint}.alerts.tsv")
        # .v3.alerts.tsv
        minutes_path = os.path.join(self.outdir, f"{self.mint}.minutes.tsv")
        events_jsonl_path = os.path.join(self.outdir, f"{self.mint}.events.jsonl")
        v3_alerts_path = os.path.join(self.outdir, f"{self.mint}.v3.alerts.tsv")
        v3_evidence_path = os.path.join(self.outdir, f"{self.mint}.v3.evidence.jsonl")
        minutes_jsonl_path = os.path.join(self.outdir, f"{self.mint}.minutes.jsonl")
        delta_feed_path = os.path.join(self.outdir, f"{self.mint}.delta_feed.txt")
        self.meta_path = os.path.join(self.outdir, f"{self.mint}.meta.json")
        self.wallet_first_seen_path = os.path.join(self.outdir, f"{self.mint}.wallet_first_seen.tsv")
        
        if fresh:
            for path in (events_path, alerts_path, v3_alerts_path, minutes_path, events_jsonl_path, v3_evidence_path, minutes_jsonl_path, delta_feed_path, self.meta_path, self.wallet_first_seen_path):
                if os.path.exists(path):
                    os.remove(path)
        
        os.makedirs(self.outdir, exist_ok=True)
        self.load_wallet_first_seen()
        
        self.events_file = open(events_path, 'a', buffering=1)
        if os.path.getsize(events_path) == 0:
            self.events_file.write("ts,ts_iso,mint,wallet,side,token_amt,sig\n")
        
        self.alerts_file = open(alerts_path, 'a', buffering=1)
        if os.path.getsize(alerts_path) == 0:
            self.alerts_file.write("ts_iso\tsignal\tseverity\tglance_text\tcontext\n")

        self.v3_alerts_file = open(v3_alerts_path, 'a', buffering=1)
        if os.path.getsize(v3_alerts_path) == 0:
            self.v3_alerts_file.write("ts_iso\tmint\temit_type\tcategory\tconfidence\tsubject_type\tsubject_id\tsubject_members\ttrigger_type\ttrigger_id\tcorr_ids\tintel\twarning\tevidence_status\tevidence_summary\n")
        
        self.minutes_file = open(minutes_path, 'a', buffering=1)
        if os.path.getsize(minutes_path) == 0:
            self.minutes_file.write("ts_min_iso\tevents\tunique_wallets\tnew_wallets\tbuy_vol\tsell_vol\tnet_vol\tsell_buy_ratio\ttop1_share\ttop5_share\tsymmetry_share\tflip_b2s\tflip_s2b\ttop1_buy_share_1m\ttop1_buyer_wallet_1m\tactor_top1_dominant_1m\ttop1_persistent_3m\ttop1_persistent_wallet_3m\n")
        
        self.events_jsonl_file = open(events_jsonl_path, 'a', buffering=1)
        self.v3_evidence_file = open(v3_evidence_path, 'a', buffering=1)
        self.minutes_jsonl_file = open(minutes_jsonl_path, 'a', buffering=1)
        self.delta_feed_file = open(delta_feed_path, 'a', buffering=1, encoding="utf-8", newline="\n")
    
    def close_files(self):
        """Close all file handles"""
        if self.events_file:
            self.events_file.close()
        if self.alerts_file:
            self.alerts_file.close()
        if self.v3_alerts_file:
            self.v3_alerts_file.close()
        if self.minutes_file:
            self.minutes_file.close()
        if self.events_jsonl_file:
            self.events_jsonl_file.close()
        if self.v3_evidence_file:
            self.v3_evidence_file.close()
        if self.minutes_jsonl_file:
            self.minutes_jsonl_file.close()
        if self.delta_feed_file:
            self.delta_feed_file.close()

    def stop(self):
        self._stop = True

    def load_wallet_first_seen(self):
        """Load persistent wallet first-seen data from TSV (append-only)."""
        if not os.path.exists(self.wallet_first_seen_path):
            with open(self.wallet_first_seen_path, 'a', buffering=1) as handle:
                handle.write("wallet\tfirst_seen_ts_iso\tfirst_seen_minute_iso\n")
            return

        with open(self.wallet_first_seen_path, 'r') as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("wallet\t"):
                    continue
                wallet, ts_iso, minute_iso = line.split("\t")
                self.wallet_first_seen[wallet] = (ts_iso, minute_iso)

    def append_wallet_first_seen_rows(self, rows):
        """Append new wallet first-seen rows to TSV."""
        with open(self.wallet_first_seen_path, 'a', buffering=1) as handle:
            if os.path.getsize(self.wallet_first_seen_path) == 0:
                handle.write("wallet\tfirst_seen_ts_iso\tfirst_seen_minute_iso\n")
            for wallet, ts_iso, minute_iso in rows:
                handle.write(f"{wallet}\t{ts_iso}\t{minute_iso}\n")
    
    def fetch_page(self):
        """Fetch latest transactions from Helius.

        NOTE: We intentionally do NOT rely on `until` for forward pagination.
        Live mode is implemented by fetching the latest page and de-duping via `seen_sigs`.
        """
        params = {
            'api-key': self.helius_key,
            'limit': 100
        }

        try:
            resp = requests.get(self.helius_url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"ERROR fetching page: {e}")
            return None

    def get_fee_payer(self, tx):
        """Resolve the transaction actor (fee payer).

        This is the ONLY wallet identity Panda should anchor to for actor-based intelligence.
        """
        fp = tx.get('feePayer')
        if fp:
            return fp

        t = tx.get('transaction') or {}
        msg = t.get('message') or {}
        keys = msg.get('accountKeys') or []
        if not keys:
            return None

        first = keys[0]
        if isinstance(first, str):
            return first
        if isinstance(first, dict) and first.get('pubkey'):
            return first['pubkey']
        return None

    def classify_side_for_actor(self, transfer, actor):
        """Determine BUY/SELL relative to actor.

        BUY  => actor is the receiver (toUserAccount == actor)
        SELL => actor is the sender   (fromUserAccount == actor)
        else => ignore (not actor's trade action)
        """
        to_acc = transfer.get('toUserAccount')
        from_acc = transfer.get('fromUserAccount')

        if to_acc == actor:
            return 'BUY'
        if from_acc == actor:
            return 'SELL'
        return None

    def parse_transaction(self, tx):
        """Extract actor-anchored BUY/SELL events from a transaction"""
        events = []

        ts = tx.get('timestamp')
        if not ts:
            return events

        sig = tx.get('signature', '')
        if not sig:
            return events

        actor = self.get_fee_payer(tx)
        if not actor:
            # If actor cannot be resolved deterministically, do not guess.
            return events

        token_transfers = tx.get('tokenTransfers', [])

        for transfer in token_transfers:
            if transfer.get('mint') != self.mint:
                continue

            token_amt = float(transfer.get('tokenAmount', 0))
            if token_amt == 0:
                continue

            side = self.classify_side_for_actor(transfer, actor)
            if side is None:
                continue

            wallet = actor  # anchor to actor, not pool/vault/program accounts
            events.append((ts, wallet, side, token_amt, sig))

        return events

    def write_event(self, ts, wallet, side, token_amt, sig):
        """Write event to CSV immediately"""
        ts_iso = datetime.fromtimestamp(ts, tz=SGT).isoformat()
        self.events_file.write(f"{ts},{ts_iso},{self.mint},{wallet},{side},{token_amt},{sig}\n")

    def write_event_jsonl(self, ts_iso, minute_bucket, wallet, side, token_amt, sig, is_new_wallet):
        event_payload = {
            "ts_iso": ts_iso,
            "minute_bucket": minute_bucket,
            "wallet": wallet,
            "side": side.lower(),
            "amount": token_amt,
            "signature": sig or "",
            "is_new_wallet": is_new_wallet
        }
        self.events_jsonl_file.write(json.dumps(event_payload, separators=(',', ':')) + "\n")
    
    def emit_alert(self, signal, severity, glance_text, context_dict):
        """Emit a sparse alert"""
        ts_iso = datetime.now(SGT).isoformat()
        context = ' '.join(f"{k}={v}" for k, v in context_dict.items())
        self.alerts_file.write(f"{ts_iso}\t{signal}\t{severity}\t{glance_text}\t{context}\n")
        
        emoji_map = {
            'PUMP_IGNITION': '🟢',
            'PUMP_BUILDING': '🟡', 
            'PUMP_EXHAUSTION': '🟠',
            'DUMP_IGNITION': '🟠',
            'DUMP_CONFIRM': '🔴',
            'DUMP_EXHAUSTION': '🔵',
            'CONTROL_ON': '🟣',
            'CONTROL_OFF': '⚪',
            'UPSIDE_IGNITION': '✨',
            'EXPANSION_UNDER_PRESSURE': '🔥',
            'REPLACEMENT_DOMINANCE': '💥',
            'BROADENING_CONTROL': '📈',
            'STRUCTURAL_FATIGUE': '🌫️',
            'WHALE_BURST_BUY': '🐋',
            'WHALE_BURST_SELL': '🐋'
        }
        emoji = emoji_map.get(signal, '⚪')
        time_str = datetime.now(SGT).strftime('%H:%M:%S')
        self.legacy_outbox.append(f"{emoji} {signal:20s} {time_str}  {glance_text}")
        self.alerts_emitted.append((time_str, signal))

    def _format_delta_emit(self, emit):
        time_str = datetime.fromtimestamp(emit["ts"], tz=SGT).strftime("%H:%M:%S")
        lines = [
            f"{time_str}  {emit['dimension']} {emit['arrow']}",
            f"      {emit['primary']}",
            f"      {emit['context']}"
        ]
        if emit["scream"]:
            lines = [line.upper() for line in lines]
        return lines
    
    def get_or_create_bucket(self, minute_ts):
        """Get or create a minute bucket"""
        if minute_ts not in self.minute_buckets:
            self.minute_buckets[minute_ts] = {
                'events': 0,
                'wallets': set(),
                'new_wallets': set(),
                'buy_vol': 0.0,
                'sell_vol': 0.0,
                'wallet_buy_vol': defaultdict(float),
                'wallet_sell_vol': defaultdict(float),
                'wallet_total_vol': defaultdict(float),
                'flip_buy_to_sell': set(),
                'flip_sell_to_buy': set()
            }
        return self.minute_buckets[minute_ts]
    
    def update_minute_bar(self, ts, wallet, side, token_amt):
        """Update minute bucket statistics"""
        minute_iso = datetime.fromtimestamp(ts, tz=SGT).replace(second=0, microsecond=0).isoformat()
        bucket = self.get_or_create_bucket(minute_iso)
        
        bucket['events'] += 1
        bucket['wallets'].add(wallet)
        
        # Track new wallets (first seen in persistent store)
        ts_iso = datetime.fromtimestamp(ts, tz=SGT).isoformat()
        if wallet not in self.wallet_first_seen:
            self.wallet_first_seen[wallet] = (ts_iso, minute_iso)
            bucket['new_wallets'].add(wallet)
            self.append_wallet_first_seen_rows([(wallet, ts_iso, minute_iso)])
        
        # Volume tracking
        if side == 'BUY':
            bucket['buy_vol'] += token_amt
            bucket['wallet_buy_vol'][wallet] += token_amt
            
            # Check for flip: sold earlier in session, now buying
            if wallet in self.wallet_has_sold and wallet not in self.wallet_has_bought:
                bucket['flip_sell_to_buy'].add(wallet)
            
            self.wallet_has_bought.add(wallet)
        else:
            bucket['sell_vol'] += token_amt
            bucket['wallet_sell_vol'][wallet] += token_amt
            
            # Check for flip: bought earlier in session, now selling
            if wallet in self.wallet_has_bought and wallet not in self.wallet_has_sold:
                bucket['flip_buy_to_sell'].add(wallet)
            
            self.wallet_has_sold.add(wallet)
        
        bucket['wallet_total_vol'][wallet] += token_amt
    
    def mark_minute_complete(self, minute_ts):
        """Mark a minute as complete and add to sorted list"""
        if minute_ts not in self.completed_minutes:
            self.completed_minutes.append(minute_ts)
            self.completed_minutes.sort()
    
    def write_minute_bar(self, minute_ts):
        """Write a single completed minute bar to file"""
        if minute_ts not in self.minute_buckets:
            return
        
        bucket = self.minute_buckets[minute_ts]
        
        ts_iso = minute_ts
        events = bucket['events']
        unique_wallets = len(bucket['wallets'])
        new_wallets = len(bucket['new_wallets'])
        buy_vol = bucket['buy_vol']
        sell_vol = bucket['sell_vol']
        net_vol = buy_vol - sell_vol
        sell_buy_ratio = sell_vol / max(buy_vol, EPS)
        
        # Top wallet concentration
        sorted_vols = sorted(bucket['wallet_total_vol'].values(), reverse=True)
        total_vol = buy_vol + sell_vol
        top1_share = sorted_vols[0] / max(total_vol, EPS) if sorted_vols else 0
        top5_vol = sum(sorted_vols[:5])
        top5_share = top5_vol / max(total_vol, EPS)

        # Top buyer dominance (BUY volume only)
        if buy_vol <= 0 or not bucket['wallet_buy_vol']:
            top1_buy_share_1m = 0.0
            top1_buyer_wallet_1m = ""
            actor_top1_dominant_1m = 0
        else:
            top1_buyer_wallet_1m, max_buy_vol = max(
                bucket['wallet_buy_vol'].items(),
                key=lambda item: item[1]
            )
            top1_buy_share_1m = max_buy_vol / max(buy_vol, EPS)
            actor_top1_dominant_1m = 1 if top1_buy_share_1m >= 0.55 else 0

        def top1_buyer_wallet_for_minute(target_minute_ts):
            target_bucket = self.minute_buckets.get(target_minute_ts)
            if not target_bucket:
                return ""
            target_buy_vol = target_bucket['buy_vol']
            if target_buy_vol <= 0 or not target_bucket['wallet_buy_vol']:
                return ""
            return max(target_bucket['wallet_buy_vol'].items(), key=lambda item: item[1])[0]

        completed_index = self.completed_minutes.index(minute_ts) if minute_ts in self.completed_minutes else -1
        prev_minute_1 = self.completed_minutes[completed_index - 1] if completed_index >= 1 else None
        prev_minute_2 = self.completed_minutes[completed_index - 2] if completed_index >= 2 else None

        w1 = top1_buyer_wallet_for_minute(prev_minute_1) if prev_minute_1 else ""
        w2 = top1_buyer_wallet_for_minute(prev_minute_2) if prev_minute_2 else ""

        if top1_buyer_wallet_1m and top1_buyer_wallet_1m == w1 and top1_buyer_wallet_1m == w2:
            top1_persistent_3m = 1
            top1_persistent_wallet_3m = top1_buyer_wallet_1m
        else:
            top1_persistent_3m = 0
            top1_persistent_wallet_3m = ""
        
        # Symmetry calculation (per-wallet net-flat churn)
        symmetry_vol = 0.0
        for wallet in bucket['wallets']:
            v_buy = bucket['wallet_buy_vol'][wallet]
            v_sell = bucket['wallet_sell_vol'][wallet]
            v_tot = bucket['wallet_total_vol'][wallet]
            symmetry_w = 1.0 - abs(v_buy - v_sell) / max(v_tot, EPS)
            if symmetry_w >= 0.8:
                symmetry_vol += v_tot
        
        symmetry_share = symmetry_vol / max(total_vol, EPS)
        
        flip_b2s = len(bucket['flip_buy_to_sell'])
        flip_s2b = len(bucket['flip_sell_to_buy'])
        
        self.minutes_file.write(
            f"{ts_iso}\t{events}\t{unique_wallets}\t{new_wallets}\t"
            f"{buy_vol:.2f}\t{sell_vol:.2f}\t{net_vol:.2f}\t{sell_buy_ratio:.3f}\t"
            f"{top1_share:.3f}\t{top5_share:.3f}\t{symmetry_share:.3f}\t"
            f"{flip_b2s}\t{flip_s2b}\t"
            f"{top1_buy_share_1m:.3f}\t{top1_buyer_wallet_1m}\t{actor_top1_dominant_1m}\t"
            f"{top1_persistent_3m}\t{top1_persistent_wallet_3m}\n"
        )

        minute_payload = {
            "ts_min_iso": ts_iso,
            "events": events,
            "unique_wallets": unique_wallets,
            "new_wallets": new_wallets,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "net_vol": net_vol,
            "sell_buy_ratio": sell_buy_ratio,
            "top1_share": top1_share,
            "top5_share": top5_share,
            "symmetry_share": symmetry_share,
            "flip_b2s": flip_b2s,
            "flip_s2b": flip_s2b,
            "top1_buy_share_1m": top1_buy_share_1m,
            "top1_buyer_wallet_1m": top1_buyer_wallet_1m,
            "actor_top1_dominant_1m": actor_top1_dominant_1m,
            "top1_persistent_3m": top1_persistent_3m,
            "top1_persistent_wallet_3m": top1_persistent_wallet_3m
        }
        self.minutes_jsonl_file.write(json.dumps(minute_payload, separators=(',', ':')) + "\n")
    
    def get_K_metrics(self):
        """Get metrics from recent activity buffer K (last 5 completed minutes)"""
        if len(self.completed_minutes) < MIN_BUCKETS:
            return None
        
        k_minutes = self.completed_minutes[-K_MINUTES:]
        k_buckets = [self.minute_buckets[m] for m in k_minutes]
        
        # Aggregate metrics across K
        buy_vol_K = sum(b['buy_vol'] for b in k_buckets)
        sell_vol_K = sum(b['sell_vol'] for b in k_buckets)
        net_vol_K = buy_vol_K - sell_vol_K
        events_K = sum(b['events'] for b in k_buckets)
        
        unique_wallets_K = len(set().union(*[b['wallets'] for b in k_buckets]))
        new_wallets_K = len(set().union(*[b['new_wallets'] for b in k_buckets]))
        
        # New buy metrics
        new_wallets_set = set().union(*[b['new_wallets'] for b in k_buckets])
        new_buy_vol_K = 0.0
        for bucket in k_buckets:
            for wallet in new_wallets_set:
                new_buy_vol_K += bucket['wallet_buy_vol'][wallet]
        new_buy_share_K = new_buy_vol_K / max(buy_vol_K, EPS)
        
        # Flip counts
        flip_buy_to_sell_K = sum(len(b['flip_buy_to_sell']) for b in k_buckets)
        flip_sell_to_buy_K = sum(len(b['flip_sell_to_buy']) for b in k_buckets)
        
        # Control metrics (symmetry and concentration)
        wallet_buy = defaultdict(float)
        wallet_sell = defaultdict(float)
        wallet_total = defaultdict(float)
        
        for bucket in k_buckets:
            for wallet in bucket['wallets']:
                wallet_buy[wallet] += bucket['wallet_buy_vol'][wallet]
                wallet_sell[wallet] += bucket['wallet_sell_vol'][wallet]
                wallet_total[wallet] += bucket['wallet_total_vol'][wallet]
        
        total_vol_K = buy_vol_K + sell_vol_K
        
        # Symmetry share
        symmetry_vol = 0.0
        for wallet in wallet_total:
            v_buy = wallet_buy[wallet]
            v_sell = wallet_sell[wallet]
            v_tot = wallet_total[wallet]
            symmetry_w = 1.0 - abs(v_buy - v_sell) / max(v_tot, EPS)
            if symmetry_w >= 0.8:
                symmetry_vol += v_tot
        symmetry_share_K = symmetry_vol / max(total_vol_K, EPS)
        
        # Top 5 concentration
        sorted_vols = sorted(wallet_total.values(), reverse=True)
        top5_vol = sum(sorted_vols[:5]) if len(sorted_vols) >= 5 else sum(sorted_vols)
        top5_share_K = top5_vol / max(total_vol_K, EPS)
        
        return {
            'buy_vol_K': buy_vol_K,
            'sell_vol_K': sell_vol_K,
            'net_vol_K': net_vol_K,
            'events_K': events_K,
            'unique_wallets_K': unique_wallets_K,
            'new_wallets_K': new_wallets_K,
            'new_buy_share_K': new_buy_share_K,
            'flip_buy_to_sell_K': flip_buy_to_sell_K,
            'flip_sell_to_buy_K': flip_sell_to_buy_K,
            'symmetry_share_K': symmetry_share_K,
            'top5_share_K': top5_share_K,
            'total_vol_K': total_vol_K
        }
    
    def get_B_metrics(self):
        """Get baseline metrics from B (last 15 completed minutes or all available)"""
        if len(self.completed_minutes) < MIN_BUCKETS:
            return None
        
        b_minutes = self.completed_minutes[-B_MINUTES:]
        b_buckets = [self.minute_buckets[m] for m in b_minutes]
        
        # Per-minute total volumes for percentile calculation
        minute_vols = [b['buy_vol'] + b['sell_vol'] for b in b_buckets]
        minute_vols.sort()
        
        # 90th percentile
        p90_idx = min(len(minute_vols) - 1, int(len(minute_vols) * 0.9)) if minute_vols else 0
        p90_total_vol_1m_B = minute_vols[p90_idx] if minute_vols else 0
        
        # Per-wallet per-minute max volumes for whale detection
        wallet_minute_vols = []
        for bucket in b_buckets:
            if bucket['wallet_total_vol']:
                wallet_minute_vols.append(max(bucket['wallet_total_vol'].values()))
        wallet_minute_vols.sort()
        
        # 95th percentile
        p95_idx = min(len(wallet_minute_vols) - 1, int(len(wallet_minute_vols) * 0.95)) if wallet_minute_vols else 0
        p95_wallet_vol_1m_B = wallet_minute_vols[p95_idx] if wallet_minute_vols else 0
        
        return {
            'p90_total_vol_1m_B': p90_total_vol_1m_B,
            'p95_wallet_vol_1m_B': p95_wallet_vol_1m_B
        }
    
    def _compute_baseline_thresholds(self, b_buckets):
        events_1m = []
        new_wallets_1m = []
        unique_wallets_1m = []
        top5_share_1m = []
        new_buy_share_1m = []

        for bucket in b_buckets:
            events_1m.append(bucket['events'])
            new_wallets_1m.append(len(bucket['new_wallets']))
            unique_wallets_1m.append(len(bucket['wallets']))
            total_vol_1m = bucket['buy_vol'] + bucket['sell_vol']

            sorted_vols = sorted(bucket['wallet_total_vol'].values(), reverse=True)
            top5_vol = sum(sorted_vols[:5])
            top5_share_1m.append(top5_vol / max(total_vol_1m, EPS))

            new_buy_vol_1m = sum(bucket['wallet_buy_vol'][w] for w in bucket['new_wallets'])
            new_buy_share_1m.append(new_buy_vol_1m / max(bucket['buy_vol'], EPS))

        def pct(sorted_vals, p):
            if not sorted_vals:
                return 0
            idx = int(p * (len(sorted_vals) - 1))
            return sorted_vals[idx]

        events_1m.sort()
        new_wallets_1m.sort()
        unique_wallets_1m.sort()
        top5_share_1m.sort()
        new_buy_share_1m.sort()

        return {
            'p80_events_1m_B': pct(events_1m, 0.80),
            'p90_events_1m_B': pct(events_1m, 0.90),
            'p80_new_wallets_1m_B': pct(new_wallets_1m, 0.80),
            'p80_unique_wallets_1m_B': pct(unique_wallets_1m, 0.80),
            'p70_new_buy_share_1m_B': pct(new_buy_share_1m, 0.70),
            'p70_top5_share_1m_B': pct(top5_share_1m, 0.70)
        }
    
    def _is_upside_ignition(self, K, B, participation_expanding, inflow_persistent, latest_total_vol, latest_events_1m, latest_new_wallets_1m, p90_events_1m_B, p80_new_wallets_1m_B, p70_new_buy_share_1m_B):
        return (
            participation_expanding and
            inflow_persistent and
            (latest_total_vol >= B['p90_total_vol_1m_B'] or latest_events_1m >= max(5, p90_events_1m_B)) and
            (latest_new_wallets_1m >= max(2, p80_new_wallets_1m_B) or K['new_wallets_K'] >= max(3, int(p80_new_wallets_1m_B * K_MINUTES))) and
            (K['new_buy_share_K'] >= max(0.12, p70_new_buy_share_1m_B))
        )
    
    def analyze_signals(self):
        """Upside wallet intelligence — trader-centric, wallet-pure, state-transition based."""
        if len(self.completed_minutes) < MIN_BUCKETS:
            return

        K = self.get_K_metrics()
        B = self.get_B_metrics()
        if not K or not B:
            return

        now = time.time()

        b_minutes = self.completed_minutes[-B_MINUTES:]
        b_buckets = [self.minute_buckets[m] for m in b_minutes]
        baselines = self._compute_baseline_thresholds(b_buckets)
        p80_events_1m_B = baselines['p80_events_1m_B']
        p90_events_1m_B = baselines['p90_events_1m_B']
        p80_new_wallets_1m_B = baselines['p80_new_wallets_1m_B']
        p80_unique_wallets_1m_B = baselines['p80_unique_wallets_1m_B']
        p70_new_buy_share_1m_B = baselines['p70_new_buy_share_1m_B']
        p70_top5_share_1m_B = baselines['p70_top5_share_1m_B']

        # Latest completed minute (for whale checks + optional spot checks)
        latest_minute = self.completed_minutes[-1]
        latest_bucket = self.minute_buckets[latest_minute]
        latest_total_vol = latest_bucket['buy_vol'] + latest_bucket['sell_vol']
        latest_events_1m = latest_bucket['events']
        latest_new_wallets_1m = len(latest_bucket['new_wallets'])
        latest_unique_wallets_1m = len(latest_bucket['wallets'])

        latest_sorted_vols = sorted(latest_bucket['wallet_total_vol'].values(), reverse=True)
        latest_top5_vol = sum(latest_sorted_vols[:5])
        latest_top5_share_1m = latest_top5_vol / max(latest_total_vol, EPS)
        latest_new_buy_vol_1m = sum(latest_bucket['wallet_buy_vol'][w] for w in latest_bucket['new_wallets'])
        latest_new_buy_share_1m = latest_new_buy_vol_1m / max(latest_bucket['buy_vol'], EPS)

        # Convenience derived facts (wallet-pure)
        has_pressure = (K['sell_vol_K'] > 0) or (K['flip_buy_to_sell_K'] > 0)
        inflow_persistent = (K['buy_vol_K'] > 0)  # do NOT require net positive
        participation_expanding = (K['events_K'] > 0) and (K['new_wallets_K'] > 0)

        # Baseline comparisons (wallet-pure; avoids price)
        vol_spike_1m = (latest_total_vol >= B['p90_total_vol_1m_B'])

        # ---------------------------------------------------------------------
        # EVENT: PARTICIPATION_IGNITION
        # Goal: catch early attention→action, even if messy (selling allowed).
        # ---------------------------------------------------------------------
        ignition = self._is_upside_ignition(
            K,
            B,
            participation_expanding,
            inflow_persistent,
            latest_total_vol,
            latest_events_1m,
            latest_new_wallets_1m,
            p90_events_1m_B,
            p80_new_wallets_1m_B,
            p70_new_buy_share_1m_B
        )

        if ignition and (now - self.signal_state['last_ignition_ts'] >= EVENT_COOLDOWN):
            self.emit_alert(
                'UPSIDE_IGNITION',
                'SPARK',
                'Fresh participation ignition',
                {
                    'eventsK': K['events_K'],
                    'newK': K['new_wallets_K'],
                    'newBuyShare': f"{K['new_buy_share_K']*100:.0f}%"
                }
            )
            self.signal_state['last_ignition_ts'] = now

        # ---------------------------------------------------------------------
        # STATE: EXPANSION_UNDER_PRESSURE (latched)
        # Selling/flip does NOT block; it is expected pressure.
        # Condition: participation + inflow persist while pressure exists.
        # ---------------------------------------------------------------------
        expansion_under_pressure = (
            inflow_persistent and
            has_pressure and
            (K['events_K'] >= max(10, int(p80_events_1m_B * K_MINUTES))) and
            (K['new_wallets_K'] >= max(3, int(p80_new_wallets_1m_B * K_MINUTES * 0.6))) and
            (K['new_buy_share_K'] >= max(0.10, p70_new_buy_share_1m_B))
        )

        if expansion_under_pressure and not self.signal_state['expansion_under_pressure']:
            self.signal_state['expansion_under_pressure'] = True
            self.emit_alert(
                'EXPANSION_UNDER_PRESSURE',
                'HEAT',
                'Expansion continues despite selling',
                {
                    'newK': K['new_wallets_K'],
                    'buyK': f"{K['buy_vol_K']:.0f}",
                    'sellK': f"{K['sell_vol_K']:.0f}",
                    'flips': K['flip_buy_to_sell_K']
                }
            )

        # ---------------------------------------------------------------------
        # STATE: REPLACEMENT_DOMINANCE (latched)
        # Condition: new wallets meaningfully contribute to buys (replacement evidence).
        # ---------------------------------------------------------------------
        replacement_dominance = (
            inflow_persistent and
            (K['new_wallets_K'] >= max(3, int(p80_new_wallets_1m_B * K_MINUTES * 0.6))) and
            (K['new_buy_share_K'] >= max(0.22, p70_new_buy_share_1m_B))
        )

        if replacement_dominance and not self.signal_state['replacement_dominance']:
            self.signal_state['replacement_dominance'] = True
            self.emit_alert(
                'REPLACEMENT_DOMINANCE',
                'FORCE',
                'Replacement dominates (new wallets driving buys)',
                {
                    'newK': K['new_wallets_K'],
                    'newBuyShare': f"{K['new_buy_share_K']*100:.0f}%",
                    'eventsK': K['events_K']
                }
            )

        # ---------------------------------------------------------------------
        # STATE: BROADENING_CONTROL (latched)
        # Condition: participation is broadening (concentration not extreme).
        # ---------------------------------------------------------------------
        broadening_control = (
            inflow_persistent and
            (K['unique_wallets_K'] >= max(8, int(p80_unique_wallets_1m_B * K_MINUTES * 0.6))) and
            (K['top5_share_K'] <= min(0.85, max(0.40, p70_top5_share_1m_B)))
        )

        if broadening_control and not self.signal_state['broadening_control']:
            self.signal_state['broadening_control'] = True
            self.emit_alert(
                'BROADENING_CONTROL',
                'BREAK',
                'Participation broadening (less top-wallet dominance)',
                {
                    'top5': f"{K['top5_share_K']*100:.0f}%",
                    'uniqK': K['unique_wallets_K'],
                    'eventsK': K['events_K']
                }
            )

        # ---------------------------------------------------------------------
        # EVENT: STRUCTURAL_FATIGUE (only failure mode)
        # Trigger ONLY when replacement/participation collapses.
        # Clears upside states.
        # ---------------------------------------------------------------------
        any_upside_state = (
            self.signal_state['expansion_under_pressure'] or
            self.signal_state['replacement_dominance'] or
            self.signal_state['broadening_control']
        )

        fatigue = (
            any_upside_state and
            (latest_events_1m <= max(2, int(p80_events_1m_B * 0.3))) and
            (latest_new_wallets_1m <= max(1, int(p80_new_wallets_1m_B * 0.3))) and
            (latest_total_vol <= max(EPS, B['p90_total_vol_1m_B'] * 0.25)) and
            (K['new_buy_share_K'] < max(0.08, p70_new_buy_share_1m_B * 0.6))
        )

        if fatigue and (now - self.signal_state['last_fatigue_ts'] >= EVENT_COOLDOWN):
            # Clear latched states
            self.signal_state['expansion_under_pressure'] = False
            self.signal_state['replacement_dominance'] = False
            self.signal_state['broadening_control'] = False

            self.emit_alert(
                'STRUCTURAL_FATIGUE',
                'FADE',
                'Replacement collapsed (upside fuel exhausted)',
                {
                    'eventsK': K['events_K'],
                    'newK': K['new_wallets_K'],
                    'newBuyShare': f"{K['new_buy_share_K']*100:.0f}%",
                    'buyK': f"{K['buy_vol_K']:.0f}",
                    'sellK': f"{K['sell_vol_K']:.0f}"
                }
            )
            self.signal_state['last_fatigue_ts'] = now

        # ---------------------------------------------------------------------
        # EVENT: WHALE_BURST_BUY (unchanged — wallet-pure)
        # ---------------------------------------------------------------------
        if latest_bucket['wallet_total_vol']:
            max_wallet_vol = max(latest_bucket['wallet_total_vol'].values())
            minute_total = latest_bucket['buy_vol'] + latest_bucket['sell_vol']
            max_wallet_share = max_wallet_vol / max(minute_total, EPS)

            if (max_wallet_share >= 0.30 and
                max_wallet_vol >= B['p95_wallet_vol_1m_B'] and
                latest_bucket['buy_vol'] >= latest_bucket['sell_vol'] * 2.0 and
                now - self.signal_state['last_whale_buy_ts'] >= WHALE_COOLDOWN):

                self.emit_alert(
                    'WHALE_BURST_BUY',
                    'FORCE',
                    'Large concentrated buy',
                    {
                        'share': f"{max_wallet_share*100:.0f}%",
                        'amt': f"{max_wallet_vol/1e6:.1f}M"
                    }
                )
                self.signal_state['last_whale_buy_ts'] = now
    
    def scan(self):
        """Main scan loop - LIVE FORWARD POLLING"""
        self.attach_epoch = int(time.time())
        self.attach_ts = datetime.fromtimestamp(self.attach_epoch, tz=SGT)
        with open(self.meta_path, 'w') as handle:
            json.dump({
                "mint": self.mint,
                "attach_epoch": self.attach_epoch,
                "attach_ts_iso": self.attach_ts.isoformat(),
                "timezone": "Asia/Singapore",
                "script": "pandascan_u.py",
                "fresh": 1 if self.fresh else 0,
                "started_local_wallclock": self.attach_ts.isoformat()
            }, handle, ensure_ascii=False, indent=2)
        print(f"=== PANDA LIVE SCANNER (SPEC v2.0) ===")
        print(f"Mint: {self.mint}")
        print(f"Started: {self.attach_ts.strftime('%H:%M:%S')}")
        print(f"Attach (SGT): {self.attach_ts.isoformat()}")
        print(f"🟢 Attached - watching for signals...")
        print()
        
        last_seen_sig = None
        poll_count = 0
        events_saved = 0
        empty_poll_streak = 0
        last_heartbeat = time.time()
        current_minute = None
        
        while True:
            if self._stop:
                break
            poll_count += 1
            
            data = self.fetch_page()
            if not data:
                empty_poll_streak += 1
                if empty_poll_streak >= MAX_CONSECUTIVE_EMPTY_POLLS:
                    self.legacy_outbox.append(f"\n💤 No activity for {MAX_CONSECUTIVE_EMPTY_POLLS} polls - stream idle (continuing)")
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            
            transactions = sorted(data, key=lambda x: x.get('timestamp', 0))
            
            if not transactions:
                empty_poll_streak += 1
                if empty_poll_streak >= MAX_CONSECUTIVE_EMPTY_POLLS:
                    self.legacy_outbox.append(f"\n💤 No activity for {MAX_CONSECUTIVE_EMPTY_POLLS} polls - stream idle (continuing)")
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            
            poll_events = 0
            
            for tx in transactions:
                sig = tx.get('signature', '')
                if not sig:
                    continue
                if sig in self.seen_sigs:
                    continue

                tx_ts = int(tx.get("timestamp", 0) or 0)
                if tx_ts and self.attach_epoch and tx_ts < self.attach_epoch:
                    self.seen_sigs.add(sig)
                    continue

                tx_events = self.parse_transaction(tx)

                # Emit ALL qualifying actor-based events in this transaction
                for ts, wallet, side, token_amt, _sig in tx_events:
                    is_new_wallet = wallet not in self.wallet_first_seen
                    ts_iso = datetime.fromtimestamp(ts, tz=SGT).isoformat()
                    minute_bucket = datetime.fromtimestamp(ts, tz=SGT).replace(second=0, microsecond=0).isoformat()
                    self.write_event(ts, wallet, side, token_amt, sig)
                    self.update_minute_bar(ts, wallet, side, token_amt)
                    self.write_event_jsonl(ts_iso, minute_bucket, wallet, side, token_amt, sig, is_new_wallet)
                    delta_emits = self.delta_engine.on_event({
                        "ts": ts,
                        "wallet": wallet,
                        "side": side,
                        "amount": token_amt,
                        "is_new_wallet": is_new_wallet
                    })
                    for emit in delta_emits:
                        lines = self._format_delta_emit(emit)
                        for line in lines:
                            print(line)
                            self.delta_feed_file.write(line + "\n")

                    poll_events += 1
                    events_saved += 1
                    last_seen_sig = sig

                    # Check if we've moved to a new minute
                    event_minute = datetime.fromtimestamp(ts, tz=SGT).replace(second=0, microsecond=0).isoformat()
                    if current_minute is None:
                        current_minute = event_minute
                    elif event_minute > current_minute:
                        # Mark previous minute as complete and write it
                        self.mark_minute_complete(current_minute)
                        self.write_minute_bar(current_minute)

                        minute_bucket = self.minute_buckets[current_minute]
                        buy_vol = minute_bucket['buy_vol']
                        sell_vol = minute_bucket['sell_vol']
                        buy_wallets = len([w for w, v in minute_bucket['wallet_buy_vol'].items() if v > 0])
                        sell_wallets = len([w for w, v in minute_bucket['wallet_sell_vol'].items() if v > 0])
                        if sell_vol > 0 and minute_bucket['wallet_sell_vol']:
                            top1_sell_share = max(minute_bucket['wallet_sell_vol'].values()) / max(sell_vol, EPS)
                        else:
                            top1_sell_share = None

                        b_metrics = self.get_B_metrics()
                        whale_threshold = b_metrics['p95_wallet_vol_1m_B'] if b_metrics else 0
                        if whale_threshold:
                            whale_sells_count = len([w for w, v in minute_bucket['wallet_sell_vol'].items() if v >= whale_threshold])
                        else:
                            whale_sells_count = None
                        panda_d_input = {
                            "ts_min_iso": current_minute,
                            "buy_notional_1m": buy_vol,
                            "sell_notional_1m": sell_vol,
                            "buy_wallets": buy_wallets,
                            "sell_wallets": sell_wallets,
                            "top1_sell_share": top1_sell_share,
                            "whale_sells_count": whale_sells_count
                        }
                        panda_d_output = self.panda_d.process_minute(panda_d_input)
                        if panda_d_output:
                            self.panda_d_outputs[current_minute] = panda_d_output

                        contexts = build_phase2_contexts(current_minute, self.minute_buckets[current_minute], whale_threshold)
                        context_printed = False
                        buy_context = next((c for c in contexts if c.side == "BUY"), None)
                        sell_context = next((c for c in contexts if c.side == "SELL"), None)
                        if buy_context:
                            context_time = datetime.fromisoformat(buy_context.minute_ts).strftime('%H:%M:%S')
                            self.legacy_outbox.append(f"[CTX]   {context_time} BUY  | wallets={buy_context.wallets} | whales={buy_context.whales} | crowd={buy_context.crowd}")
                            context_printed = True
                        if sell_context:
                            context_time = datetime.fromisoformat(sell_context.minute_ts).strftime('%H:%M:%S')
                            self.legacy_outbox.append(f"[CTX]   {context_time} SELL | wallets={sell_context.wallets} | whales={sell_context.whales} | crowd={sell_context.crowd}")
                            context_printed = True
                        if context_printed:
                            narrative = sell_context.description if sell_context else (buy_context.description if buy_context else None)
                            if narrative:
                                self.legacy_outbox.append(f"        {narrative}")

                        # Analyze signals after minute completion
                        self.alerts_emitted = []
                        self.analyze_signals()
                        if context_printed and self.alerts_emitted:
                            for alert_time, alert_name in self.alerts_emitted:
                                self.legacy_outbox.append(f"[ALERT] {alert_time} {alert_name}")

                        current_minute = event_minute

                # Mark signature seen AFTER processing the entire transaction
                self.seen_sigs.add(sig)

            # Reset empty streak if we got events
            if poll_events > 0:
                empty_poll_streak = 0
            else:
                empty_poll_streak += 1
                if empty_poll_streak >= MAX_CONSECUTIVE_EMPTY_POLLS:
                    self.legacy_outbox.append(f"\n💤 No activity for {MAX_CONSECUTIVE_EMPTY_POLLS} polls - stream idle (continuing)")
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue
            
            # Optional heartbeat every 60 seconds (silent otherwise)
            now = time.time()
            if now - self.rt_last_tick >= POLL_INTERVAL_SECONDS:
                self.rt_last_tick = now
                if current_minute and current_minute in self.minute_buckets:
                    b_metrics = self.get_B_metrics()
                    K = self.get_K_metrics()
                    if b_metrics and K:
                        b_minutes = self.completed_minutes[-B_MINUTES:]
                        b_buckets = [self.minute_buckets[m] for m in b_minutes]
                        baselines = self._compute_baseline_thresholds(b_buckets)
                        open_bucket = self.minute_buckets[current_minute]
                        latest_total_vol = open_bucket['buy_vol'] + open_bucket['sell_vol']
                        latest_events_1m = open_bucket['events']
                        latest_new_wallets_1m = len(open_bucket['new_wallets'])
                        participation_expanding = (K['events_K'] > 0) and (K['new_wallets_K'] > 0)
                        inflow_persistent = (K['buy_vol_K'] > 0)
                        ignition_rt = self._is_upside_ignition(
                            K,
                            b_metrics,
                            participation_expanding,
                            inflow_persistent,
                            latest_total_vol,
                            latest_events_1m,
                            latest_new_wallets_1m,
                            baselines['p90_events_1m_B'],
                            baselines['p80_new_wallets_1m_B'],
                            baselines['p70_new_buy_share_1m_B']
                        )
                        if ignition_rt and self.rt_last_minute_emitted != current_minute:
                            time_str = datetime.now(SGT).strftime('%H:%M:%S')
                            self.legacy_outbox.append(f"✨ [RT] UPSIDE_IGNITION   {time_str}  Fresh participation ignition (provisional)")
                            self.legacy_outbox.append(f"[RT]   {time_str} UPSIDE_IGNITION")
                            self.rt_last_minute_emitted = current_minute

            if self.legacy_outbox and now >= self.legacy_next_emit_ts:
                print(self.legacy_outbox.popleft(), flush=True)
                self.legacy_next_emit_ts = now + LEGACY_EMIT_INTERVAL_SEC

            if now - last_heartbeat > 60:
                self.legacy_outbox.append(f"⏱️  {datetime.now(SGT).strftime('%H:%M')} | {events_saved} events tracked | {len(self.completed_minutes)} minutes")
                last_heartbeat = now
            
            time.sleep(POLL_INTERVAL_SECONDS)
        
        # Final minute completion and analysis
        if current_minute is not None:
            self.mark_minute_complete(current_minute)
            self.write_minute_bar(current_minute)
            self.analyze_signals()
        
        self.legacy_outbox.append(f"\n📊 Stream summary: {events_saved} events, {len(self.completed_minutes)} minutes, {poll_count} polls")


def main():
    parser = argparse.ArgumentParser(description='PANDA - Wallet Intelligence Scanner (Spec v2.0)')
    parser.add_argument('--mint', required=True, help='Token mint address (CA)')
    parser.add_argument('--outdir', required=True, help='Output directory')
    parser.add_argument('--fresh', type=int, required=True, choices=[0, 1], help='1=new files required')
    parser.add_argument('--delta-only', type=int, default=0, choices=[0, 1], help='1=suppress legacy stdout')
    
    args = parser.parse_args()
    
    helius_key = os.getenv('HELIUS_API_KEY')
    if not helius_key:
        print("ERROR: HELIUS_API_KEY environment variable not set")
        sys.exit(1)
    
    scanner = PandaScanner(args.mint, args.outdir, helius_key, delta_only=args.delta_only == 1)

    def handle_signal(_signum, _frame):
        scanner.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        scanner.init_files(args.fresh == 1)
        try:
            scanner.scan()
        except KeyboardInterrupt:
            scanner.stop()
    finally:
        try:
            scanner.close_files()
        except Exception:
            pass
        drained = 0
        while scanner.legacy_outbox and drained < 10:
            print(scanner.legacy_outbox.popleft(), flush=True)
            drained += 1
        sys.stdout.flush()
        print("🛑 Stopped - graceful exit.")


if __name__ == '__main__':
    main()
