#pragma once

#include <Common/CacheBase.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/QueryResultCacheUsage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IASTHash.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <Parsers/IAST_fwd.h>
#include <base/UUID.h>

#include <condition_variable>
#include <functional>
#include <optional>
#include <unordered_map>

namespace DB
{

struct Settings;

/// Checks that query cache can be used for query.
/// Only use the query cache if the query does not contain non-deterministic functions or system tables (which are typically non-deterministic)
/// Throws if ast contains non-deterministic functions or system tables and appropriate handling setting is set to throw.
/// When skip_context_check is true, the context's canUseQueryResultCache flag is not checked.
/// This is used for explicit per-subquery opt-in where the subquery has SETTINGS use_query_cache = true
/// but the outer query context may not have the flag set.
bool checkCanWriteQueryResultCache(ASTPtr ast, ContextPtr context, bool skip_context_check = false);

class QueryResultCacheWriter;
class QueryResultCacheReader;

/// Maps queries to query results. Useful to avoid repeated query calculation.
///
/// The cache does not aim to be transactionally consistent (which is difficult to get right). For example, the cache is not invalidated
/// when data is inserted/deleted into/from tables referenced by queries in the cache. In such situations, incorrect results may be
/// returned. In order to still obtain sufficiently up-to-date query results, a expiry time (TTL) must be specified for each cache entry
/// after which it becomes stale and is ignored. Stale entries are removed opportunistically from the cache, they are only evicted when a
/// new entry is inserted and the cache has insufficient capacity.
class QueryResultCache
{
public:
    /// Key + Entry represents a query result in the cache.
    struct Key
    {
        /// ----------------------------------------------------
        /// The actual key (data which gets hashed):

        /// The hash of the query AST.
        /// Unlike the query string, the AST is agnostic to lower/upper case (SELECT vs. select).
        IASTHash ast_hash;

        /// Note: For a transactionally consistent cache, we would need to include the system settings in the cache key or invalidate the
        /// cache whenever the settings change. This is because certain settings (e.g. "additional_table_filters") can affect the query
        /// result.

        /// ----------------------------------------------------
        /// Additional stuff data stored in the key, not hashed:

        /// Result metadata for constructing the pipe.
        SharedHeader header;

        /// The id and current roles of the user who executed the query.
        /// These members are necessary to ensure that a (non-shared, see below) entry can only be written and read by the same user with
        /// the same roles. Example attack scenarios:
        /// - after DROP USER, it must not be possible to create a new user with with the dropped user name and access the dropped user's
        ///   query result cache entries
        /// - different roles of the same user may be tied to different row-level policies. It must not be possible to switch role and
        ///   access another role's cache entries
        std::optional<UUID> user_id;
        std::vector<UUID> current_user_roles;

        /// If the associated entry can be read by other users. In general, sharing is a bad idea: First, it is unlikely that different
        /// users pose the same queries. Second, sharing potentially breaches security. E.g. User A should not be able to bypass row
        /// policies on some table by running the same queries as user B for whom no row policies exist.
        const bool is_shared;

        /// When was the entry created?
        const std::chrono::time_point<std::chrono::system_clock> created_at;

        /// When does the entry expire?
        const std::chrono::time_point<std::chrono::system_clock> expires_at;

        /// Are the chunks in the entry compressed?
        /// (we could theoretically apply compression also to the totals and extremes but it's an obscure use case)
        const bool is_compressed;

        /// The SELECT query as plain string, displayed in SYSTEM.QUERY_CACHE. Stored explicitly, i.e. not constructed from the AST, for the
        /// sole reason that QueryResultCache-related SETTINGS are pruned from the AST (see removeQueryResultCacheSettings()) which would otherwise look
        /// ugly in SYSTEM.QUERY_CACHE.
        String query_string;

        /// ID of the query.
        const String query_id;

        /// A tag (namespace) for distinguish multiple entries of the same query.
        /// This member has currently no use besides that SYSTEM.QUERY_CACHE can populate the 'tag' column conveniently without having to
        /// compute the tag from the query AST.
        const String tag;

        /// Is it subquery entry? Displayed in SYSTEM.QUERY_CACHE.
        const bool is_subquery;

        /// Ctor to construct a Key for writing into query result cache.
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            SharedHeader header_,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
            bool is_shared_,
            std::chrono::time_point<std::chrono::system_clock> created_at_,
            std::chrono::time_point<std::chrono::system_clock> expires_at_,
            bool is_compressed,
            bool is_subquery_);

        /// Ctor to construct a Key for reading from query result cache (this operation only needs the AST + user name).
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
            bool is_subquery_);

        bool operator==(const Key & other) const;
    };

    struct Entry
    {
        Chunks chunks;
        std::optional<Chunk> totals = std::nullopt;
        std::optional<Chunk> extremes = std::nullopt;
    };

private:
    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

    struct IsStale
    {
        bool operator()(const Key & key) const;
    };

public:
    /// query --> query result
    using Cache = CacheBase<Key, Entry, KeyHasher, EntryWeight>;

    QueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    void updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    QueryResultCacheReader createReader(const Key & key);
    std::shared_ptr<QueryResultCacheWriter> createWriter(
        const Key & key,
        std::chrono::milliseconds min_query_runtime,
        bool squash_partial_results,
        size_t max_block_size,
        size_t max_query_result_cache_size_in_bytes_quota,
        size_t max_query_result_cache_entries_quota);

    /// Blocks the calling thread while another, concurrently running query is writing a result for the same key into the cache
    /// ("query B waits for query A"). This allows B to reuse ("steal") the entry that A is about to insert into the cache instead of
    /// having both A and B compute the same, potentially expensive result independently.
    /// Does nothing if no such write is in progress. `is_query_cancelled` is polled periodically while waiting so that killing the
    /// calling (B's) query does not block it forever, e.g. if A's query runs into 'max_execution_time' or hangs.
    void waitForConcurrentInsert(const Key & key, const std::function<bool()> & is_query_cancelled);

    void clear(const std::optional<String> & tag);

    size_t maxSizeInBytes() const;
    size_t sizeInBytes() const;
    size_t count() const;

    /// Record new execution of query represented by key. Returns number of executions so far.
    size_t recordQueryRun(const Key & key);

    /// For debugging and system tables
    std::vector<QueryResultCache::Cache::KeyMapped> dump() const;

private:
    Cache cache; /// has its own locking --> not protected by mutex

    mutable std::mutex mutex;

    /// query --> query execution count
    using TimesExecuted = std::unordered_map<Key, size_t, KeyHasher>;
    TimesExecuted times_executed TSA_GUARDED_BY(mutex);

    /// Cache configuration
    size_t max_entry_size_in_bytes TSA_GUARDED_BY(mutex) = 0;
    size_t max_entry_size_in_rows TSA_GUARDED_BY(mutex) = 0;

    /// ------------------------------------------------------------------------------------------------------------------------
    /// Synchronization of concurrent queries which run the same (deterministic) SELECT (see waitForConcurrentInsert()). Guarded by a
    /// dedicated 'in_flight_writes_mutex' rather than 'mutex' above: cache configuration/quota bookkeeping and the concurrent-query
    /// registry are unrelated, and sharing one mutex would make them contend with each other for no reason.

    /// One instance per cache key with a write in progress. Deliberately one condition variable *per key* (as opposed to a single,
    /// cache-wide condition variable): completing the write for one key must only wake up queries waiting for that same key, not every
    /// query waiting on any other, unrelated key.
    struct InFlightWrite
    {
        std::condition_variable cv;
    };

    mutable std::mutex in_flight_writes_mutex;
    std::unordered_map<Key, std::shared_ptr<InFlightWrite>, KeyHasher> in_flight_writes TSA_GUARDED_BY(in_flight_writes_mutex);

    /// If 'write' matches the currently registered in-flight write for 'key', unregisters it and wakes up threads blocked in
    /// waitForConcurrentInsert() for that key. Called (via InFlightRegistration) at most once per registration.
    void finishWrite(const Key & key, const std::shared_ptr<InFlightWrite> & write);

public:
    /// RAII handle for a single entry in 'in_flight_writes'. While alive, the key it was constructed for counts as "being written";
    /// calling unregister() (or destroying the handle, which calls it automatically) ends this. unregister() is idempotent, i.e. safe to
    /// call any number of times (including zero, if the handle was never actually registered - e.g. because a fresh entry already
    /// existed in the cache, or because another writer had already registered for this key first), from any combination of an explicit
    /// call and the destructor: at most one QueryResultCache::finishWrite() call is ever made per registration.
    class InFlightRegistration
    {
    public:
        /// 'write_' may be null, meaning this handle does not actually hold a registration (see class comment).
        InFlightRegistration(QueryResultCache & query_result_cache_, Key key_, std::shared_ptr<InFlightWrite> write_);
        InFlightRegistration(const InFlightRegistration &) = delete;
        InFlightRegistration & operator=(const InFlightRegistration &) = delete;
        ~InFlightRegistration();

        void unregister();

    private:
        QueryResultCache & query_result_cache;
        Key key;
        std::shared_ptr<InFlightWrite> write;
    };

private:
    friend class StorageSystemQueryResultCache;
    friend class QueryResultCacheWriter;
    friend class QueryResultCacheReader;
};

/// Buffers multiple partial query result chunks (buffer()) and eventually stores them as cache entry (finalizeWrite()).
///
/// Implementation note: Queries may throw exceptions during runtime, e.g. out-of-memory errors. In this case, no query result must be
/// written into the query result cache. Unfortunately, neither the Writer nor the special transform added on top of the query pipeline
/// which holds the Writer know whether they are destroyed because the query ended successfully or because of an exception (otherwise, we
/// could simply implement a check in their destructors). To handle exceptions correctly nevertheless, we do the actual insert in
/// finalizeWrite() as opposed to the Writer destructor. This function is then called only for successful queries in finish_callback() which
/// runs before the transform and the Writer are destroyed, whereas for unsuccessful queries we do nothing (the Writer is destroyed w/o
/// inserting anything).
/// Queries may also be cancelled by the user, in which case IProcessor's cancel bit is set. FinalizeWrite() is only called if the
/// cancel bit is not set.
///
/// Synchronization of concurrent queries: While a Writer for a given key exists, 'in_flight_registration' (see
/// QueryResultCache::InFlightRegistration) keeps it registered in the owning QueryResultCache so that other, concurrently running
/// queries with the same key can wait for it to finish instead of redundantly computing the same result
/// (QueryResultCache::waitForConcurrentInsert()). The registration is undone exactly once, either explicitly at the end of
/// finalizeWrite() (the regular case) or, if finalizeWrite() is never called (exception/cancellation), implicitly by
/// 'in_flight_registration's destructor. Either way, once a Writer is unregistered, waiting queries wake up and re-probe the cache: they
/// either find the entry the Writer just inserted ("steal" it) or, if the Writer aborted or decided not to cache the result, fall back
/// to computing it themselves.
class QueryResultCacheWriter
{
public:
    QueryResultCacheWriter(const QueryResultCacheWriter &) = delete;
    QueryResultCacheWriter & operator=(const QueryResultCacheWriter &) = delete;
    /// No user-declared destructor: 'in_flight_registration' below unregisters itself automatically (see InFlightRegistration).

    enum class ChunkType : uint8_t
    {
        Result,
        Totals,
        Extremes
    };
    void buffer(Chunk && chunk, ChunkType chunk_type);

    void finalizeWrite();
private:
    using Cache = QueryResultCache::Cache;

    std::mutex mutex;
    QueryResultCache & query_result_cache;
    Cache & cache;
    const QueryResultCache::Key key;
    const size_t max_entry_size_in_bytes;
    const size_t max_entry_size_in_rows;
    const std::chrono::time_point<std::chrono::system_clock> query_start_time = std::chrono::system_clock::now(); /// Writer construction and finalizeWrite() coincide with query start/end
    const std::chrono::milliseconds min_query_runtime;
    const bool squash_partial_results;
    const size_t max_block_size;
    Cache::MappedPtr query_result TSA_GUARDED_BY(mutex) = std::make_shared<QueryResultCache::Entry>();
    std::atomic<bool> skip_insert = false;
    std::atomic<bool> was_finalized = false;
    /// RAII registration of 'key' as being written (see QueryResultCache::InFlightRegistration); a no-op handle (as opposed to an empty
    /// std::optional) if this Writer never became the one query allowed to insert 'key', e.g. because a fresh entry already existed in
    /// the cache, or another writer registered for it first.
    QueryResultCache::InFlightRegistration in_flight_registration;
    LoggerPtr logger = getLogger("QueryResultCache");

    /// 'skip_insert_' and 'in_flight_write_' are determined by QueryResultCache::createWriter() while it holds
    /// QueryResultCache::in_flight_writes_mutex, i.e. atomically with respect to other concurrent createWriter()/waitForConcurrentInsert()
    /// calls for the same key.
    QueryResultCacheWriter(
        QueryResultCache & query_result_cache_,
        Cache & cache_,
        const Cache::Key & key_,
        bool skip_insert_,
        std::shared_ptr<QueryResultCache::InFlightWrite> in_flight_write_,
        size_t max_entry_size_in_bytes_,
        size_t max_entry_size_in_rows_,
        std::chrono::milliseconds min_query_runtime_,
        bool squash_partial_results_,
        size_t max_block_size_);

    friend class QueryResultCache; /// for createWriter()
};

/// Reader's constructor looks up a query result for a key in the cache. If found, it constructs source processors (that generate the
/// cached result) for use in a pipe or query pipeline.
class QueryResultCacheReader
{
public:
    using Cache = QueryResultCache::Cache;

    bool hasCacheEntryForKey(bool update_profile_events = true) const;

    /// Must only be called if hasCacheEntryForKey is true
    std::chrono::time_point<std::chrono::system_clock> entryCreatedAt();
    std::chrono::time_point<std::chrono::system_clock> entryExpiresAt();

    /// getSource*() moves source processors out of the Reader. Call each of these method just once.
    std::unique_ptr<SourceFromChunks> getSource();
    std::unique_ptr<SourceFromChunks> getSourceExtremes();
    std::unique_ptr<SourceFromChunks> getSourceTotals();

private:
    QueryResultCacheReader(Cache & cache_, const Cache::Key & key, const std::lock_guard<std::mutex> &);
    void buildSourceFromChunks(SharedHeader header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes);

    std::unique_ptr<SourceFromChunks> source_from_chunks;
    std::unique_ptr<SourceFromChunks> source_from_chunks_totals;
    std::unique_ptr<SourceFromChunks> source_from_chunks_extremes;

    std::chrono::time_point<std::chrono::system_clock> created_at;
    std::chrono::time_point<std::chrono::system_clock> expires_at;

    LoggerPtr logger = getLogger("QueryResultCache");

    friend class QueryResultCache; /// for createReader()
};


using QueryResultCachePtr = std::shared_ptr<QueryResultCache>;

}
