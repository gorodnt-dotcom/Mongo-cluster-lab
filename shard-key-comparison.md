# Shard Key Comparison: hashed `productId` vs compound `{ productId, viewTimestamp }`

Methodology, workload parameters, and setup are in [README.md](README.md#synthetic-workload) and [README.md](README.md#document-model). This file is the test results.

Generated with `scripts/workload_and_shard_key_comparison.py` against the live 3-config-server / 2-shard (2 members each) cluster in this repo. The same 500,000-event synthetic dataset (seed=42, so it's deterministic and reproducible) was loaded into two collections that differ only in shard key:

- `maplegrove.productViews_hashed` — shard key `{ productId: "hashed" }`
- `maplegrove.productViews_compound` — shard key `{ productId: 1, viewTimestamp: 1 }`

Re-ran the full pipeline twice (once mid-development, once from a fully fresh cluster after fixing bugs) and got identical numbers both times, confirming the pipeline is deterministic and reproducible, not a one-off fluke.

## Shard distribution

| Collection | shard1rs | shard2rs |
|---|---|---|
| `productViews_hashed` (`{ productId: "hashed" }`) | 243,965 docs (48.79%) | 256,035 docs (51.21%) |
| `productViews_compound` (`{ productId: 1, viewTimestamp: 1 }`) | 260,169 docs (52.03%) | 239,831 docs (47.97%) |

Both land close to a 50/50 split — but for different reasons, and with different guarantees:

- **Hashed `productId`** distributes near-evenly *automatically*. MongoDB pre-splits and evenly assigns chunks across shards the moment you shard an empty collection with a hashed key, regardless of which productIds turn out to be hot. No manual work needed.
- **Compound `{ productId, viewTimestamp }`** only ended up balanced because the test deliberately (a) scattered the 250 hot SKUs randomly across the id space rather than clustering them, and (b) manually pre-split and moved a chunk before loading. Left alone, an ascending range key starts as a single chunk on one shard and depends on the balancer to catch up — slower, and not automatically even, especially at this small (~100MB) data size, which sits well under the default 128MB chunk-size threshold that would otherwise trigger auto-splitting.

**Important nuance the assignment's 5%/60% skew is designed to surface: neither key eliminates hot-shard risk for a single very popular SKU.** Both schemes use `productId` as (part of) the shard key, so a given SKU's documents always live on exactly one shard — under hashing because `hash(productId)` is deterministic, under the range key because that SKU's id falls in exactly one chunk range. If one specific SKU accounts for a large share of the 60% hot-tier traffic, that shard takes a disproportionate share of the writes either way. Sharding on `productId` balances *SKUs* across shards, not *traffic* — the near-even split above only holds because there are 250 hot SKUs spread across 2 shards, not because either key specifically protects against one dominant hot key.

Where the two keys genuinely differ is **query routing**, below.

## Access patterns

Measured via `explain("executionStats")` against both collections — same dataset, same query, only the shard key differs.

| # | Access pattern | Filter | hashed `productId` | compound `{productId, viewTimestamp}` |
|---|---|---|---|---|
| P1 | Most recent views for one product (e.g. product analytics page) | `productId = X`, sort by `viewTimestamp` desc, limit 50 | `SINGLE_SHARD` (shard2rs), 50 keys / 50 docs examined for 50 returned | `SINGLE_SHARD` (shard1rs), 50 keys / 50 docs examined for 50 returned |
| P2 | Views for one product within a time window | `productId = X`, `viewTimestamp` in range | `SINGLE_SHARD` (shard2rs), 3/3/3 | `SINGLE_SHARD` (shard1rs), 3/3/3 |
| P3 | Global recent activity across all products (real-time dashboard) | `viewTimestamp >= now - 10m`, sort desc, limit 100 | `SHARD_MERGE_SORT` on both shards, **500,000 docs examined** for 123 returned | `SHARD_MERGE_SORT` on both shards, **500,000 docs examined** for 123 returned |
| P4 | All views by one customer | `customerId = Y` | `SHARD_MERGE` on both shards, 9/9/9 | `SHARD_MERGE` on both shards, 9/9/9 |

(Sample values used: hot SKU `SKU-00913`, customerId `25000`.)

### Takeaways

- **P1 & P2 (query includes `productId`)**: both keys route to a single shard. A hashed shard key still supports single-shard targeting for *equality* matches on the shard key field — it just can't use the hash for range/sort. That's why `productViews_hashed` needed an extra secondary index, `{ productId: 1, viewTimestamp: 1 }`, to get this result: the exact index the compound collection already gets **for free** as its shard key index. That's the real cost of choosing hashed here — an extra index to build and maintain.
- **P3 (no `productId` in the query)**: identical, and identically bad, on both. Neither shard key has `viewTimestamp` as a leading/only field, so `mongos` can't target a subset of shards, and both scatter-gather a full collection scan (500,000 docs examined, 0 keys examined — a COLLSCAN) on every shard. Neither shard key choice serves this pattern; a real system would add a `{ viewTimestamp: 1 }` index (accepting the scatter-gather, but at least avoiding the collection scan) or maintain a separate time-series/summary collection for dashboards.
- **P4 (no `productId` in the query)**: identical on both, again. `customerId` isn't part of either shard key, so both fan out to every shard regardless. A local `{ customerId: 1 }` index (added on both collections) keeps each shard's local lookup cheap, but the fan-out itself is unavoidable with either shard key.

## Verdict

For this workload, **the compound key `{ productId, viewTimestamp }` is the better choice**. It matches the actual primary access pattern (look up by product, optionally narrowed by time — P1/P2) with one index instead of two, and P1/P2 are the patterns the assignment calls out as the main read path.

The hashed key's edge would show up in a different scenario: writes concentrated on a narrow, contiguous slice of *newly created* productIds (e.g., auto-incrementing SKU ids for hot new launches), where a range key would create a genuine write hotspot on the single chunk owning that slice. That case isn't really present here, since SKU ids are pre-existing and the hot SKUs are scattered across the id space, not sequential.
