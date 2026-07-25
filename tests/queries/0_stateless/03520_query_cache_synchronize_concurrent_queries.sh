#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, long
# Tag no-parallel: Messes with internal cache
#     no-fasttest: Query cache tests are excluded from fasttest, see e.g. 02494_query_cache_nested_query_bug.sh
#     long: Query artificially sleeps for a few seconds and runs many concurrent clients

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# --- Scenario 1: many concurrent, identical queries must collapse into a single Write plus many Reads -----------------------------

# Disambiguates the query below from other, unrelated queries which may show up in system.query_log
rnd=$(tr -dc 'a-z' </dev/urandom | head -c 12)
NUM_FOLLOWERS=50

QUERY="SELECT sleep(2), '$rnd' SETTINGS use_query_cache = 1, query_cache_synchronize_concurrent_queries = 1"

# Start one query and give it a head start so that it is guaranteed to become the query which actually computes the (artificially slow)
# result and inserts it into the query cache (the "leader"). Registering as leader happens almost immediately, long before the artificial
# sleep(2) call returns, so a small head start suffices.
${CLICKHOUSE_CLIENT} --query "$QUERY" > /dev/null &
sleep 1

# Now start many more, concurrently running instances of the very same query (the "followers"). None of them must run the expensive
# computation a second time; instead, each of them should wait for the leader to finish and then reuse ("steal") its result.
for _ in $(seq 1 $NUM_FOLLOWERS); do
    ${CLICKHOUSE_CLIENT} --query "$QUERY" > /dev/null &
done

wait

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "Number of query_log entries with query_cache_usage = 'Write' (must be 1 -- only the leader computed and cached the result):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query LIKE '%$rnd%' AND type = 'QueryFinish' AND query_cache_usage = 'Write'"

echo "Number of query_log entries with query_cache_usage = 'Read' (must be $NUM_FOLLOWERS -- all followers reused the leader's result):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query LIKE '%$rnd%' AND type = 'QueryFinish' AND query_cache_usage = 'Read'"

echo "Number of query_cache entries for this query (must be 1 -- no duplicate/redundant inserts happened):"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_cache WHERE query LIKE '%$rnd%'"

echo "Sum of ProfileEvents['QueryCacheSynchronizedQueries'] across all queries (must be $NUM_FOLLOWERS, one per follower):"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(ProfileEvents['QueryCacheSynchronizedQueries'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query LIKE '%$rnd%' AND type = 'QueryFinish'"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# --- Scenario 2: concurrent in-flight writes for two different keys must not interfere with each other ----------------------------

QUERY_A="SELECT sleep(2) + 100 SETTINGS use_query_cache = 1, query_cache_synchronize_concurrent_queries = 1"
QUERY_B="SELECT sleep(2) + 200 SETTINGS use_query_cache = 1, query_cache_synchronize_concurrent_queries = 1"

${CLICKHOUSE_CLIENT} --query "$QUERY_A" > /dev/null &
${CLICKHOUSE_CLIENT} --query "$QUERY_B" > /dev/null &
sleep 1

for _ in {1..5}; do
    ${CLICKHOUSE_CLIENT} --query "$QUERY_A" > /dev/null &
    ${CLICKHOUSE_CLIENT} --query "$QUERY_B" > /dev/null &
done

wait

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "Key A (query_cache_usage, count): must be exactly [('Read',5),('Write',1)]"
${CLICKHOUSE_CLIENT} --query "
    SELECT query_cache_usage, count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query LIKE '%+ 100%' AND type = 'QueryFinish'
    GROUP BY query_cache_usage ORDER BY query_cache_usage"

echo "Key B (query_cache_usage, count): must be exactly [('Read',5),('Write',1)]"
${CLICKHOUSE_CLIENT} --query "
    SELECT query_cache_usage, count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query LIKE '%+ 200%' AND type = 'QueryFinish'
    GROUP BY query_cache_usage ORDER BY query_cache_usage"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# --- Scenario 3: if the leader query fails before it can insert its result, followers must not hang ---------------------------------

rnd2=$(tr -dc 'a-z' </dev/urandom | head -c 12)
QUERY2="SELECT sleep(1), throwIf(1), '$rnd2' SETTINGS use_query_cache = 1, query_cache_synchronize_concurrent_queries = 1"

${CLICKHOUSE_CLIENT} --query "$QUERY2" > /dev/null 2>&1 &
sleep 0.3

echo "Follower of a failing leader must return promptly with the same error (not hang):"
timeout 30 ${CLICKHOUSE_CLIENT} --query "$QUERY2" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"

wait

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# --- Scenario 4: KILL QUERY on the leader must not hang followers, they must compute the result themselves --------------------------

QUERY3="SELECT sleep(3) SETTINGS use_query_cache = 1, query_cache_synchronize_concurrent_queries = 1"
leader_query_id="leader_kill_test_$$"

${CLICKHOUSE_CLIENT} --query_id "$leader_query_id" --query "$QUERY3" > /dev/null 2>&1 &
sleep 0.3

# Kill the leader while it is still "computing" (sleeping).
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$leader_query_id' SYNC" > /dev/null

echo "Follower of a KILLed leader must return promptly (not hang) with the correct, self-computed result:"
timeout 30 ${CLICKHOUSE_CLIENT} --query "$QUERY3"

wait

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# --- Scenario 5: a concurrent SYSTEM DROP QUERY CACHE must not cause a hang or an incorrect result -----------------------------------

QUERY4="SELECT sleep(2) + 41 SETTINGS use_query_cache = 1, query_cache_synchronize_concurrent_queries = 1"

${CLICKHOUSE_CLIENT} --query "$QUERY4" > /dev/null &
sleep 0.5

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

echo "Result must still be correct despite a concurrent SYSTEM DROP QUERY CACHE:"
timeout 30 ${CLICKHOUSE_CLIENT} --query "$QUERY4"

wait

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
