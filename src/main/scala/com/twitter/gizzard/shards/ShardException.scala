package com.twitter.gizzard.shards

import com.twitter.xrayspecs.Duration

/**
 * Base class for all exceptions thrown by a shard operation.
 */
class ShardException(description: String, cause: Throwable) extends Exception(description, cause) {
  def this(description: String) = this(description, null)
}

/**
 * Shard exceptions that aren't interesting as stack traces. They refer to matter-of-course
 * timeouts or rejections that aren't code errors. Any "cause" exception is thrown away, and
 * no stack trace is filled in.
 */
class NormalShardException(description: String, shardId: ShardId, cause: Throwable) extends
      ShardException(description + ": " + shardId, null) {
  def this(description: String, shardId: ShardId) = this(description, shardId, null)

  override def fillInStackTrace() = this
}

/**
 * Shard timed out while waiting for a query response. This may or may not be a retryable error,
 * depending on if the database is just overloaded, or if the query is intrinsically too complex
 * to complete in the desired time.
 */
class ShardTimeoutException(val timeout: Duration, shardId: ShardId, cause: Throwable) extends
      NormalShardException("Timeout (" + timeout.inMillis + " msec)", shardId, cause) {
  def this(timeout: Duration, shardId: ShardId) = this(timeout, shardId, null)
}

/**
 * Shard timed out while waiting for a database connection. This may or may not be a retryable
 * error, depending on whether the database connection pool is oversubscribed, or the database is
 * offline.
 */
class ShardDatabaseTimeoutException(timeout: Duration, shardId: ShardId, cause: Throwable)
      extends ShardTimeoutException(timeout, shardId, cause) {
  def this(timeout: Duration, shardId: ShardId) = this(timeout, shardId, null)
}

/**
 * Shard refused to do the operation, possibly because it's blocked. This is not a retryable error.
 *
 * Often this exception is used to signal a ReplicatingShard that it should try another shard,
 * because this shard is read-only, write-only, or blocked (offline).
 */
class ShardRejectedOperationException(description: String, shardId: ShardId) extends
  NormalShardException(description, shardId)

/**
 * Shard cannot do the operation because all possible child shards are unavailable. This is only
 * thrown by a ReplicatingShard. This is not a retryable error.
 */
class ShardOfflineException(shardId: ShardId) extends
  NormalShardException("All shard replicas are down for shard", shardId)

/**
 * Shard would like to be skipped for reads & writes. If all shards within a replica do this, then
 * the write is "thrown away" and the exception is passed up.
 */
class ShardBlackHoleException(shardId: ShardId) extends
  NormalShardException("Shard is blackholed", shardId, null)

/**
 * A replicating shard timed out while waiting for a response to a write request to one of the
 * replica shards. This is a "future" timeout and indicates that the replication future timeout
 * is lower than your per-database write timeout. It only occurs when doing parallel writes.
 */
class ReplicatingShardTimeoutException(shardId: ShardId, ex: Throwable)
  extends NormalShardException("Timeout waiting for write to shard", shardId, ex)
