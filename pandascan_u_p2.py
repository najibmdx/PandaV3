"""Phase-2 Actor Coordination Context (no stdout)."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Phase2Context:
    minute_ts: str
    side: str
    wallets: int
    whales: int
    crowd: int
    description: str


def _describe(side, wallets, whales, crowd):
    if wallets == 0:
        return None
    if side == "SELL" and whales == 1 and crowd >= 1:
        return "Selling led by one large wallet with followers."
    if whales == 1 and crowd == 0:
        return "Activity narrowed to a single large wallet."
    if whales == 1 and crowd >= 1:
        return "Broad participation with one large wallet present."
    if whales == 0 and wallets >= 3:
        return "Participation widened with no whale control."
    if whales == 0 and wallets >= 2:
        return "Many small wallets acted together."
    return None


def build_phase2_contexts(minute_ts, bucket, whale_threshold):
    contexts = []
    whale_threshold = whale_threshold or 0

    side_specs = (
        ("BUY", bucket['wallet_buy_vol']),
        ("SELL", bucket['wallet_sell_vol'])
    )

    for side, wallet_vols in side_specs:
        wallets = [w for w, v in wallet_vols.items() if v > 0]
        whale_wallets = [w for w in wallets if bucket['wallet_total_vol'].get(w, 0) >= whale_threshold]
        wallets_count = len(wallets)
        whales_count = len(whale_wallets)
        crowd_count = wallets_count - whales_count

        description = _describe(side, wallets_count, whales_count, crowd_count)
        if description:
            contexts.append(Phase2Context(
                minute_ts=minute_ts,
                side=side,
                wallets=wallets_count,
                whales=whales_count,
                crowd=crowd_count,
                description=description
            ))

    return contexts
