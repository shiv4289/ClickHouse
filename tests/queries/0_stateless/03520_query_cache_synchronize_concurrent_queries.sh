#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, long
# Tag no-parallel: Messes with internal cache
#     no-fasttest: Query cache tests are excluded from fasttest, see e.g. 02494_query_cache_nested_query_bug.sh
#     long: Query artificially sleeps for a few seconds

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# Disambiguates the query below from other, unrelated queries which may show up in system.query_log
rnd=$(tr -dc 'a-z' </dev/urandom | head -c 12)

QUERY="SELECT sleep(2), '$rnd' SETTINGS use_query_cache = 1, query_cache_synchronize_concurrent_queries = 1"

# Start one query and give it a head start so that it is guaranteed to become the query which actually computes the (artificially slow)
# result and inserts it into the query cache (the "leader"). Registering as leader happens almost immediately, long before the artificial
# sleep(2) call returns, so a small head start suffices.
${CLICKHOUSE_CLIENT} --query "$QUERY" > /dev/null &
sleep 1

# Now start four more, concurrently running instances of the very same query (the "followers"). None of them must run the expensive
# computation a second time; instead, each of them should wait for the leader to finish and then reuse ("steal") its result.
for _ in {1..4}; do
    ${CLICKHOUSE_CLIENT} --query "$QUERY" > /dev/null &
done

wait

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "Number of query_log entries with query_cache_usage = 'Write' (must be 1 -- only the leader computed and cached the result):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query LIKE '%$rnd%' AND type = 'QueryFinish' AND query_cache_usage = 'Write'"

echo "Number of query_log entries with query_cache_usage = 'Read' (must be 4 -- all followers reused the leader's result):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query LIKE '%$rnd%' AND type = 'QueryFinish' AND query_cache_usage = 'Read'"

echo "Number of query_cache entries for this query (must be 1 -- no duplicate/redundant inserts happened):"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_cache WHERE query LIKE '%$rnd%'"

echo "Sum of ProfileEvents['QueryCacheSynchronizedQueries'] across all 5 queries (must be 4, one per follower):"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(ProfileEvents['QueryCacheSynchronizedQueries'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query LIKE '%$rnd%' AND type = 'QueryFinish'"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# If the leader query fails (or is cancelled) before it can insert its result into the cache, any followers waiting for it must not hang
# forever -- they must wake up and compute the result (incl. the failure) themselves.
rnd2=$(tr -dc 'a-z' </dev/urandom | head -c 12)
QUERY2="SELECT sleep(1), throwIf(1), '$rnd2' SETTINGS use_query_cache = 1, query_cache_synchronize_concurrent_queries = 1"

${CLICKHOUSE_CLIENT} --query "$QUERY2" > /dev/null 2>&1 &
sleep 0.3

echo "Follower of a failing leader must return promptly with the same error (not hang):"
timeout 30 ${CLICKHOUSE_CLIENT} --query "$QUERY2" 2>&1 | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"

wait

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
