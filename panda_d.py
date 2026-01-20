"""Panda-D downside intelligence module (no stdout, no I/O)."""

EPS = 1e-9


class PandaD:
    def __init__(self):
        self._sbr_history = []

    def process_minute(self, minute_data):
        ts_min_iso = minute_data.get("ts_min_iso")
        buy_notional = float(minute_data.get("buy_notional_1m", 0) or 0)
        sell_notional = float(minute_data.get("sell_notional_1m", 0) or 0)
        buy_wallets = int(minute_data.get("buy_wallets", 0) or 0)
        sell_wallets = int(minute_data.get("sell_wallets", 0) or 0)
        top1_sell_share = minute_data.get("top1_sell_share")
        whale_sells_count = minute_data.get("whale_sells_count")

        if buy_notional == 0 and sell_notional == 0:
            return {
                "pressure_state": "NO CLEAR CONTROL",
                "ts_min_iso": ts_min_iso,
                "buy_wallets": buy_wallets,
                "sell_wallets": sell_wallets,
                "buy_notional": buy_notional,
                "sell_notional": sell_notional,
                "sell_buy_ratio": 0.0,
                "net_sell_notional": 0.0,
                "top1_sell_share": top1_sell_share,
                "whale_sells_count": whale_sells_count
            }

        sbr = sell_notional / max(buy_notional, EPS)
        nsn = sell_notional - buy_notional

        self._sbr_history.append(sbr)
        if len(self._sbr_history) > 3:
            self._sbr_history.pop(0)

        release = False
        if sbr <= 0.67:
            release = True
        elif len(self._sbr_history) >= 2 and self._sbr_history[-1] <= 1.10 and self._sbr_history[-2] <= 1.10:
            release = True

        persistent_overwhelm = False
        persistent_control = False

        if not release:
            last_three = self._sbr_history[-3:]
            overwhelm_count = sum(1 for v in last_three if v >= 1.50)
            control_count = sum(1 for v in last_three if v >= 1.10)
            if overwhelm_count >= 2:
                persistent_overwhelm = True
            else:
                count_control_only = sum(1 for v in last_three if 1.10 <= v < 1.50)
                if overwhelm_count >= 1 and (count_control_only >= 1 or overwhelm_count >= 2):
                    persistent_overwhelm = True
            if control_count >= 2:
                persistent_control = True

        if persistent_overwhelm:
            pressure_state = "SELLING OVERWHELMS BUYERS"
        elif persistent_control:
            pressure_state = "SELLERS IN CONTROL"
        else:
            if sbr <= 0.67:
                pressure_state = "BUYERS IN CONTROL"
            elif sbr < 0.90:
                pressure_state = "FRAGILE BUYING"
            elif sbr <= 1.10:
                pressure_state = "NO CLEAR CONTROL"
            elif sbr < 1.50:
                pressure_state = "SELLERS IN CONTROL"
            else:
                pressure_state = "SELLING OVERWHELMS BUYERS"

        return {
            "pressure_state": pressure_state,
            "ts_min_iso": ts_min_iso,
            "buy_wallets": buy_wallets,
            "sell_wallets": sell_wallets,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "sell_buy_ratio": sbr,
            "net_sell_notional": nsn,
            "top1_sell_share": top1_sell_share,
            "whale_sells_count": whale_sells_count
        }
