-- Tags: no-parallel

DROP TABLE IF EXISTS test.mark_cache_test;

-- Temporarily reduce index mark cache size to force eviction
SET index_mark_cache_size = 1048576; -- 1 MB

-- Create a MergeTree table
CREATE TABLE test.mark_cache_test
(
    id UInt64,
    value String
)
    ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;

-- Insert enough data to exceed cache limit
INSERT INTO test.mark_cache_test
SELECT number, toString(number)
FROM numbers(500000);

-- Multiple selects to simulate pressure and cause evictions
SELECT count() FROM test.mark_cache_test WHERE id % 10 = 0;
SELECT count() FROM test.mark_cache_test WHERE id % 10 = 1;
SELECT count() FROM test.mark_cache_test WHERE id % 10 = 2;

-- Verify custom mark cache eviction metrics
SELECT
    event,
    value
FROM system.events
WHERE event IN (
                'MarkCacheEvictedFiles',
                'MarkCacheEvictedMarks',
                'MarkCacheEvictedBytes'
    )
ORDER BY event;

-- Resets index mark cache size to default, just in case
SET index_mark_cache_size = 536870912;

-- Clean up
DROP TABLE test.mark_cache_test;
