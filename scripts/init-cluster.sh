#!/usr/bin/env bash
set -euo pipefail

wait_for() {
  local host="$1" port="$2"
  echo "Waiting for $host:$port..."
  until mongosh --quiet --host "$host" --port "$port" --eval "db.runCommand({ ping: 1 })" >/dev/null 2>&1; do
    sleep 2
  done
  echo "$host:$port is up."
}

wait_for_primary() {
  local host="$1" port="$2"
  echo "Waiting for a primary in the replica set at $host:$port..."
  until mongosh --quiet --host "$host" --port "$port" --eval '
    rs.status().members.some(function (m) { return m.state === 1; })
  ' 2>/dev/null | grep -q true; do
    sleep 2
  done
  echo "Primary elected for replica set at $host:$port."
}

wait_for cfg1 27019
wait_for cfg2 27019
wait_for cfg3 27019
wait_for shard1a 27018
wait_for shard1b 27018
wait_for shard2a 27020
wait_for shard2b 27020

echo "Initiating config server replica set (3 members)..."
mongosh --quiet --host cfg1 --port 27019 --eval '
  rs.initiate({
    _id: "cfgrs",
    configsvr: true,
    members: [
      { _id: 0, host: "cfg1:27019" },
      { _id: 1, host: "cfg2:27019" },
      { _id: 2, host: "cfg3:27019" }
    ]
  })
'

echo "Initiating shard1 replica set (2 members)..."
mongosh --quiet --host shard1a --port 27018 --eval '
  rs.initiate({
    _id: "shard1rs",
    members: [
      { _id: 0, host: "shard1a:27018" },
      { _id: 1, host: "shard1b:27018" }
    ]
  })
'

echo "Initiating shard2 replica set (2 members)..."
mongosh --quiet --host shard2a --port 27020 --eval '
  rs.initiate({
    _id: "shard2rs",
    members: [
      { _id: 0, host: "shard2a:27020" },
      { _id: 1, host: "shard2b:27020" }
    ]
  })
'

wait_for_primary cfg1 27019
wait_for_primary shard1a 27018
wait_for_primary shard2a 27020

wait_for mongos 27017

echo "Adding shards to the cluster via mongos..."
mongosh --quiet --host mongos --port 27017 --eval '
  sh.addShard("shard1rs/shard1a:27018,shard1b:27018");
  sh.addShard("shard2rs/shard2a:27020,shard2b:27020");
'

echo "Enabling sharding on the maplegrove database..."
mongosh --quiet --host mongos --port 27017 --eval '
  sh.enableSharding("maplegrove");
'

echo "Cluster setup complete. Shard status:"
mongosh --quiet --host mongos --port 27017 --eval 'sh.status()'
