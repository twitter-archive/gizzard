package com.twitter.gizzard.shards

import com.twitter.xrayspecs.Duration

/**
 * Base class for all exceptions thrown by a shard operation.
 */
class ShardException(description: String, cause: Throwable) extends Exception(description, cause) {
  def this(description: String) = this(description, null)
}

/**
 * Shard timed out while waiting for a query response. This may or may not be a retryable error,
 * depending on if the database is just overloaded, or if the query is intrinsically too complex
 * to complete in the desired time.
 */
class ShardTimeoutException(val timeout: Duration, cause: Throwable) extends ShardException("timeout: " + timeout.inMillis + " msec", cause)

/**
 * Shard timed out while waiting for a database connection. This is a retryable error.
 */
class ShardDatabaseTimeoutException(timeout: Duration, cause: Throwable) extends ShardTimeoutException(timeout, cause)

/**
 * Shard refused to do the operation, possibly because it's blocked. This is not a retryable error.
 *
 * Often this exception is used to signal a ReplicatingShard that it should try another shard,
 * because this shard is read-only, write-only, or blocked (offline).
 */
class ShardRejectedOperationException(description: String) extends ShardException(description)

/**
 * Shard cannot do the operation because all possible child shards are unavailable. This is only
 * thrown by a ReplicatingShard. This is not a retryable error.
 */
class ShardOfflineException extends ShardException("All shard replicas are down")
