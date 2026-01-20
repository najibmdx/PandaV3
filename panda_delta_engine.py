"""Event-based delta emit engine (library only, no I/O)."""

from collections import deque


class DeltaEngine:
    def __init__(self):
        self._events_2m = deque()
        self._events_10m = deque()
        self._buy_vol_2m = 0.0
        self._sell_vol_2m = 0.0
        self._wallet_vol_10m = {}
        self._total_vol_10m = 0.0
        self._first_seen_ts = {}
        self._new_wallets_10m = set()
        self._first_seen_deque = deque()
        self._new_wallets_series = deque()
        self._new_wallets_sum = 0.0
        self._top1_series = deque()
        self._states = {
            "Interest": "→",
            "Pressure": "→",
            "Control": "→"
        }
        self._candidates = {
            "Interest": {"state": None, "count": 0, "since": None},
            "Pressure": {"state": None, "count": 0, "since": None},
            "Control": {"state": None, "count": 0, "since": None}
        }
        self._last_emit_ts = {
            "Interest": None,
            "Pressure": None,
            "Control": None
        }
        self._last_scream_ts = {
            "Interest": None,
            "Pressure": None,
            "Control": None
        }
        self._last_interest_up_ts = None
        self._last_trigger_ts = {
            "Interest": None,
            "Pressure": None,
            "Control": None
        }
        self._dwell_seconds = {
            "Pressure": 20,
            "Interest": 30,
            "Control": 45
        }
        self._pending = {
            "Interest": {"arrow": None, "count": 0, "since": None},
            "Pressure": {"arrow": None, "count": 0, "since": None},
            "Control": {"arrow": None, "count": 0, "since": None}
        }

    def on_event(self, event, explain=False):
        ts = int(event["ts"])
        wallet = event["wallet"]
        side = event["side"].upper()
        amount = float(event["amount"])
        is_new_wallet = bool(event.get("is_new_wallet", False))

        self._update_windows(ts, wallet, side, amount, is_new_wallet)

        emits = []
        emits.extend(self._eval_interest(ts, explain))
        emits.extend(self._eval_pressure(ts, explain))
        emits.extend(self._eval_control(ts, explain))
        return emits

    def flush(self):
        return []

    def _update_windows(self, ts, wallet, side, amount, is_new_wallet):
        cutoff_2m = ts - 120
        while self._events_2m and self._events_2m[0][0] < cutoff_2m:
            ev_ts, ev_side, ev_amount = self._events_2m.popleft()
            if ev_side == "BUY":
                self._buy_vol_2m -= ev_amount
            else:
                self._sell_vol_2m -= ev_amount

        self._events_2m.append((ts, side, amount))
        if side == "BUY":
            self._buy_vol_2m += amount
        else:
            self._sell_vol_2m += amount

        cutoff_10m = ts - 600
        while self._events_10m and self._events_10m[0][0] < cutoff_10m:
            ev_ts, ev_wallet, ev_amount = self._events_10m.popleft()
            self._wallet_vol_10m[ev_wallet] -= ev_amount
            self._total_vol_10m -= ev_amount
            if self._wallet_vol_10m[ev_wallet] <= 0:
                del self._wallet_vol_10m[ev_wallet]

        self._events_10m.append((ts, wallet, amount))
        self._wallet_vol_10m[wallet] = self._wallet_vol_10m.get(wallet, 0.0) + amount
        self._total_vol_10m += amount

        if is_new_wallet and wallet not in self._first_seen_ts:
            self._first_seen_ts[wallet] = ts
            self._first_seen_deque.append((ts, wallet))
            self._new_wallets_10m.add(wallet)

        cutoff_new = ts - 600
        while self._first_seen_deque and self._first_seen_deque[0][0] < cutoff_new:
            _, old_wallet = self._first_seen_deque.popleft()
            if old_wallet in self._new_wallets_10m:
                self._new_wallets_10m.remove(old_wallet)

        self._new_wallets_series.append((ts, len(self._new_wallets_10m)))
        self._new_wallets_sum += len(self._new_wallets_10m)
        cutoff_30m = ts - 1800
        while self._new_wallets_series and self._new_wallets_series[0][0] < cutoff_30m:
            _, val = self._new_wallets_series.popleft()
            self._new_wallets_sum -= val

        top1_now = 0.0
        if self._total_vol_10m > 0 and self._wallet_vol_10m:
            top1_now = max(self._wallet_vol_10m.values()) / self._total_vol_10m
        self._top1_series.append((ts, top1_now))
        while self._top1_series and self._top1_series[0][0] < cutoff_30m:
            self._top1_series.popleft()

    def _mean_new_wallets(self):
        if not self._new_wallets_series:
            return 0.0
        return self._new_wallets_sum / len(self._new_wallets_series)

    def _top1_at(self, threshold_ts):
        value = None
        for ts, share in reversed(self._top1_series):
            if ts <= threshold_ts:
                value = share
                break
        if value is None and self._top1_series:
            value = self._top1_series[0][1]
        return value if value is not None else 0.0

    def _eval_interest(self, ts, explain):
        new_wallets_10m = len(self._new_wallets_10m)
        baseline_new_wallets = self._mean_new_wallets()
        r_interest = new_wallets_10m / max(1.0, baseline_new_wallets)

        arrow = None
        severe = False
        if r_interest >= 2.0 and new_wallets_10m >= 25:
            arrow = "▲▲"
        elif 1.35 <= r_interest < 2.0 and new_wallets_10m >= 10:
            arrow = "▲"
        elif 0.80 <= r_interest < 1.35:
            arrow = "→"
        elif r_interest < 0.80 and new_wallets_10m <= baseline_new_wallets - 8:
            arrow = "▼"

        if arrow == "▼" and r_interest < 0.50 and new_wallets_10m <= 5:
            if self._last_interest_up_ts is not None and ts - self._last_interest_up_ts <= 600:
                severe = True

        if arrow in ("▲", "▲▲"):
            self._last_interest_up_ts = ts

        if arrow is None:
            return []

        context_map = {
            "▲": "Participation rising.",
            "▲▲": "Participation surging.",
            "→": "Participation steady.",
            "▼": "Participation weakening."
        }
        if severe:
            context = "Participation collapsing."
        else:
            context = context_map[arrow]

        primary = f"Wallets entering: {new_wallets_10m} (10m)"
        return self._maybe_emit("Interest", arrow, context, primary, ts, explain, severe)

    def _eval_pressure(self, ts, explain):
        epsilon = 0.01
        ratio_2m = (self._sell_vol_2m + epsilon) / (self._buy_vol_2m + epsilon)
        arrow = None
        if ratio_2m >= 2.0 and self._sell_vol_2m >= 10:
            arrow = "▲▲"
        elif 1.20 <= ratio_2m < 2.0 and self._sell_vol_2m >= 3:
            arrow = "▲"
        elif 0.85 <= ratio_2m < 1.20:
            arrow = "→"
        elif ratio_2m < 0.85 and self._buy_vol_2m >= 3:
            arrow = "▼"

        if arrow is None:
            return []

        if arrow == "▲" and self._last_emit_ts["Pressure"] is not None:
            if ts - self._last_emit_ts["Pressure"] >= 180 and ratio_2m >= 1.50:
                arrow = "▲▲"

        context_map = {
            "▲": "Sell pressure building.",
            "▲▲": "Sell dominance extreme.",
            "→": "Flow balanced.",
            "▼": "Buy-led flow."
        }
        primary = f"Sell/buy: {ratio_2m:.2f}"
        return self._maybe_emit("Pressure", arrow, context_map[arrow], primary, ts, explain, False)

    def _eval_control(self, ts, explain):
        top1_now = self._top1_series[-1][1] if self._top1_series else 0.0
        delta_top1_10m = (top1_now - self._top1_at(ts - 600)) * 100.0
        delta_top1_2m = (top1_now - self._top1_at(ts - 120)) * 100.0

        arrow = None
        label_window = "10m"
        if delta_top1_10m <= 0 and delta_top1_2m <= 0:
            if delta_top1_10m <= -1.0:
                arrow = "▼"
            else:
                arrow = "→"
        elif delta_top1_2m >= 2.0 or delta_top1_10m >= 4.0:
            arrow = "▲▲"
            if delta_top1_2m >= 2.0:
                label_window = "2m"
        elif 1.0 <= delta_top1_10m < 4.0:
            arrow = "▲"
        elif -1.0 < delta_top1_10m < 1.0:
            arrow = "→"
        elif delta_top1_10m <= -1.0:
            arrow = "▼"

        if arrow is None:
            return []

        context_map = {
            "▲": "Ownership concentrating.",
            "▲▲": "Rapid concentration.",
            "→": "Concentration stable.",
            "▼": "Ownership dispersing."
        }
        if label_window == "2m":
            primary_delta = delta_top1_2m
        else:
            primary_delta = delta_top1_10m
        primary = f"Top1 share delta ({label_window}): {primary_delta:.2f} pp"
        return self._maybe_emit("Control", arrow, context_map[arrow], primary, ts, explain, False)

    def _maybe_emit(self, dimension, arrow, context, primary, ts, explain, severe):
        current = self._states[dimension]
        candidate = self._candidates[dimension]

        if arrow == current:
            pending = self._pending[dimension]
            pending["arrow"] = None
            pending["count"] = 0
            pending["since"] = None
            candidate["state"] = None
            candidate["count"] = 0
            candidate["since"] = None
            return []

        scream = False
        if dimension == "Pressure" and arrow == "▲▲":
            scream = True
        if dimension == "Control" and arrow == "▲▲":
            scream = True
        if dimension == "Interest" and arrow == "▼" and severe:
            scream = True

        if dimension == "Interest" and arrow == "▼":
            trigger = "Interest"
        elif dimension == "Pressure" and arrow == "▲":
            trigger = "Pressure"
        elif dimension == "Control" and arrow == "▲":
            trigger = "Control"
        else:
            trigger = None

        if trigger:
            for other in ("Interest", "Pressure", "Control"):
                if other == trigger:
                    continue
                other_ts = self._last_trigger_ts.get(other)
                if other_ts is not None and ts - other_ts <= 180:
                    scream = True
                    break
            self._last_trigger_ts[trigger] = ts

        if scream:
            last_scream = self._last_scream_ts[dimension]
            if last_scream is None or ts - last_scream >= 60:
                self._states[dimension] = arrow
                self._last_emit_ts[dimension] = ts
                self._last_scream_ts[dimension] = ts
                pending = self._pending[dimension]
                pending["arrow"] = None
                pending["count"] = 0
                pending["since"] = None
                return [self._format_emit(dimension, arrow, primary, context, True, explain, ts)]

        pending = self._pending[dimension]
        if pending["arrow"] != arrow:
            pending["arrow"] = arrow
            pending["count"] = 1
            pending["since"] = ts
            return []
        pending["count"] += 1

        last_emit = self._last_emit_ts[dimension]
        dwell_seconds = self._dwell_seconds[dimension]
        if last_emit is not None and ts - last_emit < dwell_seconds:
            if pending["count"] < 2:
                return []
            if ts - pending["since"] < dwell_seconds:
                return []

        if candidate["state"] != arrow:
            candidate["state"] = arrow
            candidate["count"] = 1
            candidate["since"] = ts
        else:
            candidate["count"] += 1

        if candidate["count"] >= 3 or (candidate["since"] is not None and ts - candidate["since"] >= dwell_seconds):
            self._states[dimension] = arrow
            self._last_emit_ts[dimension] = ts
            pending["arrow"] = None
            pending["count"] = 0
            pending["since"] = None
            candidate["state"] = None
            candidate["count"] = 0
            candidate["since"] = None
            return [self._format_emit(dimension, arrow, primary, context, False, explain, ts)]

        return []

    def _format_emit(self, dimension, arrow, primary, context, scream, explain, ts):
        emit = {
            "ts": ts,
            "dimension": dimension,
            "arrow": arrow,
            "primary": primary,
            "context": context,
            "scream": scream,
            "explain": explain
        }
        return emit
