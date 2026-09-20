#!/usr/bin/env python3
"""
Generates the MapleGrove synthetic product-view workload, loads it into two
identically-populated sharded collections (one keyed on a hashed productId,
one on a compound {productId, viewTimestamp}), and reports measured evidence
for the shard-key comparison and the four access patterns in the README.
"""
import argparse
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from bson import MaxKey, MinKey
from pymongo import ASCENDING, MongoClient

DB_NAME = "maplegrove"
HASHED_COLL = "productViews_hashed"
COMPOUND_COLL = "productViews_compound"

REFERRERS = ["search", "recommendation", "direct", "email", "social"]
DEVICES = ["mobile", "desktop", "tablet"]


def sku_id(n):
    return f"SKU-{n:05d}"


def generate_events(num_events, num_customers, num_skus, hot_fraction, hot_view_fraction, window_days, seed):
    """
    Document model: one 'view' event per document.
    {
      customerId: int,       # 0..num_customers-1
      productId: str,        # "SKU-00001".."SKU-0<num_skus>"
      viewTimestamp: date,   # uniformly distributed over the trailing window_days
      sessionId: str,        # uuid4, one per event (no session grouping modeled)
      referrer: str,
      deviceType: str,
    }

    hot_fraction of SKUs are scattered randomly across the id space (not the
    first N ids) and jointly receive hot_view_fraction of all views, which is
    the realistic case: popularity is independent of when/how an id was
    assigned.
    """
    rng = random.Random(seed)
    num_hot = max(1, round(num_skus * hot_fraction))
    all_skus = list(range(1, num_skus + 1))
    hot_skus = rng.sample(all_skus, num_hot)
    cold_skus = [s for s in all_skus if s not in set(hot_skus)]

    num_hot_views = round(num_events * hot_view_fraction)
    num_cold_views = num_events - num_hot_views

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=window_days)

    def random_ts():
        offset = rng.uniform(0, window_days * 86400)
        return window_start + timedelta(seconds=offset)

    def make_event(sku_pool):
        return {
            "customerId": rng.randrange(num_customers),
            "productId": sku_id(rng.choice(sku_pool)),
            "viewTimestamp": random_ts(),
            "sessionId": str(uuid.uuid4()),
            "referrer": rng.choice(REFERRERS),
            "deviceType": rng.choice(DEVICES),
        }

    events = [make_event(hot_skus) for _ in range(num_hot_views)]
    events += [make_event(cold_skus) for _ in range(num_cold_views)]
    rng.shuffle(events)
    return events, hot_skus, window_start, now


def load_collection(db, name, events, batch_size):
    coll = db[name]
    total = len(events)
    t0 = time.time()
    for i in range(0, total, batch_size):
        batch = [dict(e) for e in events[i:i + batch_size]]
        coll.insert_many(batch, ordered=False)
    elapsed = time.time() - t0
    print(f"  loaded {total} docs into {name} in {elapsed:.1f}s")
    return coll


def shard_distribution(db, coll_name):
    stats = db.command("collStats", coll_name)
    shards = stats.get("shards", {})
    total = sum(s["count"] for s in shards.values())
    dist = {}
    for shard_name, s in shards.items():
        count = s["count"]
        dist[shard_name] = {
            "docs": count,
            "pct": round(100 * count / total, 2) if total else 0,
        }
    return dist, total


def explain_pattern(coll, filter_, sort=None, limit=None):
    find_cmd = {"find": coll.name, "filter": filter_}
    if sort:
        find_cmd["sort"] = dict(sort)
    if limit:
        find_cmd["limit"] = limit
    plan = coll.database.command({"explain": find_cmd, "verbosity": "executionStats"})
    winning = plan.get("queryPlanner", {}).get("winningPlan", {})
    stage = winning.get("stage")
    shard_plans = winning.get("shards", [])
    shard_names = sorted(s.get("shardName") for s in shard_plans) if shard_plans else None

    exec_stats = plan.get("executionStats", {})
    keys_examined = exec_stats.get("totalKeysExamined")
    docs_examined = exec_stats.get("totalDocsExamined")
    n_returned = exec_stats.get("nReturned")

    return stage, shard_names, keys_examined, docs_examined, n_returned


def run_access_patterns(db, hot_sku, sample_customer, window_start, now):
    hashed = db[HASHED_COLL]
    compound = db[COMPOUND_COLL]

    window_mid = window_start + (now - window_start) / 2
    recent_cutoff = now - timedelta(minutes=10)

    patterns = [
        (
            "P1: most recent views for one product",
            {"productId": sku_id(hot_sku)},
            [("viewTimestamp", -1)],
            50,
        ),
        (
            "P2: views for one product within a time window",
            {
                "productId": sku_id(hot_sku),
                "viewTimestamp": {"$gte": window_mid, "$lte": window_mid + timedelta(hours=1)},
            },
            [("viewTimestamp", 1)],
            None,
        ),
        (
            "P3: global recent activity, last 10 minutes, all products",
            {"viewTimestamp": {"$gte": recent_cutoff}},
            [("viewTimestamp", -1)],
            100,
        ),
        (
            "P4: all views by one customer",
            {"customerId": sample_customer},
            None,
            None,
        ),
    ]

    results = []
    for label, filter_, sort, limit in patterns:
        h_stage, h_shards, h_keys, h_docs, h_ret = explain_pattern(hashed, filter_, sort, limit)
        c_stage, c_shards, c_keys, c_docs, c_ret = explain_pattern(compound, filter_, sort, limit)
        results.append({
            "pattern": label,
            "hashed": {"stage": h_stage, "shards": h_shards, "keysExamined": h_keys, "docsExamined": h_docs, "nReturned": h_ret},
            "compound": {"stage": c_stage, "shards": c_shards, "keysExamined": c_keys, "docsExamined": c_docs, "nReturned": c_ret},
        })
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--events", type=int, default=500_000)
    ap.add_argument("--customers", type=int, default=50_000)
    ap.add_argument("--skus", type=int, default=5_000)
    ap.add_argument("--hot-sku-fraction", type=float, default=0.05)
    ap.add_argument("--hot-view-fraction", type=float, default=0.60)
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--batch-size", type=int, default=10_000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    client = MongoClient(args.uri)
    db = client[DB_NAME]

    print(f"Generating {args.events} synthetic view events "
          f"({args.customers} customers, {args.skus} SKUs, "
          f"{args.hot_sku_fraction:.0%} of SKUs get {args.hot_view_fraction:.0%} of views)...")
    events, hot_skus, window_start, now = generate_events(
        args.events, args.customers, args.skus,
        args.hot_sku_fraction, args.hot_view_fraction,
        args.window_days, args.seed,
    )

    print(f"Sharding {HASHED_COLL} on {{ productId: 'hashed' }}...")
    client.admin.command("enableSharding", DB_NAME)
    db[HASHED_COLL].drop()
    db[HASHED_COLL].create_index([("productId", "hashed")])
    client.admin.command("shardCollection", f"{DB_NAME}.{HASHED_COLL}", key={"productId": "hashed"})

    print(f"Sharding {COMPOUND_COLL} on {{ productId: 1, viewTimestamp: 1 }}...")
    db[COMPOUND_COLL].drop()
    db[COMPOUND_COLL].create_index([("productId", ASCENDING), ("viewTimestamp", ASCENDING)])
    client.admin.command("shardCollection", f"{DB_NAME}.{COMPOUND_COLL}", key={"productId": 1, "viewTimestamp": 1})

    # The compound collection starts as a single chunk on one shard (ascending
    # range keys aren't auto-pre-split the way hashed keys are). Pre-split and
    # move half the productId range so the comparison isn't just an artifact
    # of the balancer not having run yet.
    mid_sku = args.skus // 2
    print(f"Pre-splitting {COMPOUND_COLL} at productId={sku_id(mid_sku)} and moving the upper half to shard2rs...")
    client.admin.command(
        "split", f"{DB_NAME}.{COMPOUND_COLL}",
        middle={"productId": sku_id(mid_sku), "viewTimestamp": MinKey()},
    )
    client.admin.command(
        "moveChunk", f"{DB_NAME}.{COMPOUND_COLL}",
        find={"productId": sku_id(args.skus), "viewTimestamp": MaxKey()},
        to="shard2rs",
    )

    print(f"Loading {args.events} events into {HASHED_COLL} and {COMPOUND_COLL}...")
    load_collection(db, HASHED_COLL, events, args.batch_size)
    load_collection(db, COMPOUND_COLL, events, args.batch_size)

    # The hashed collection's shard key index doesn't help sort/range on
    # viewTimestamp, so give it the same supporting index the compound
    # collection gets for free from its shard key.
    print(f"Building supporting index on {HASHED_COLL} for productId+viewTimestamp queries...")
    db[HASHED_COLL].create_index([("productId", ASCENDING), ("viewTimestamp", ASCENDING)])

    print("Building customerId index on both collections (for P4)...")
    db[HASHED_COLL].create_index([("customerId", ASCENDING)])
    db[COMPOUND_COLL].create_index([("customerId", ASCENDING)])

    print("\n=== Shard distribution ===")
    for coll_name in (HASHED_COLL, COMPOUND_COLL):
        dist, total = shard_distribution(db, coll_name)
        print(f"{coll_name} ({total} docs):")
        for shard_name, d in sorted(dist.items()):
            print(f"  {shard_name}: {d['docs']} docs ({d['pct']}%)")

    print("\n=== Access pattern routing (explain) ===")
    sample_customer = args.customers // 2
    results = run_access_patterns(db, hot_skus[0], sample_customer, window_start, now)
    for r in results:
        print(f"{r['pattern']}")
        for key in ("hashed", "compound"):
            v = r[key]
            print(f"  {key:<8}: stage={v['stage']:<16} shards={v['shards']}  "
                  f"keysExamined={v['keysExamined']} docsExamined={v['docsExamined']} nReturned={v['nReturned']}")

    print(f"\nSample hot SKU used above: {sku_id(hot_skus[0])}")
    print(f"Sample customerId used above: {sample_customer}")


if __name__ == "__main__":
    main()
