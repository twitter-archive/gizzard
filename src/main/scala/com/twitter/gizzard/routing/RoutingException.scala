package com.twitter.gizzard.routing


/**
 * Base class for all routing exceptions. Can also be used directly.
 */
class RoutingException(description: String, cause: Throwable) extends Exception(description, cause) {
  def this(description: String) = this(description, null)
}

/**
 * Routing exceptions that aren't interesting as stack traces. They refer to matter-of-course
 * timeouts or rejections that aren't code errors. Any "cause" exception is thrown away, and
 * no stack trace is filled in.
 */
class NormalRoutingException(description: String, nodeId: RoutingNodeId, cause: Throwable) extends
      RoutingException(description + ": " + nodeId, null) {
  def this(description: String, nodeId: RoutingNodeId) = this(description, nodeId, null)

  override def fillInStackTrace() = this
}

/**
 * Node refused to do the operation, possibly because it's blocked. This is not a retryable error.
 *
 * Often this exception is used to signal a ReplicatingNode that it should try another replica,
 * because this node is read-only, write-only, or blocked (offline).
 */
class NodeRejectedOperationException(description: String, nodeId: RoutingNodeId) extends
  NormalRoutingException(description, nodeId)

/**
 * Node cannot do the operation because all possible child nodes are unavailable. This is not a retryable error.
 */
class NodeOfflineException(nodeId: RoutingNodeId) extends
  NormalRoutingException("All child nodes are down for node", nodeId)

/**
 * Node would like to be skipped for reads & writes. If all child nodes of a node do this then
 * the write is "thrown away" and the exception is passed up.
 */
class NodeBlackHoleException(nodeId: RoutingNodeId) extends
  NormalRoutingException("Node is blackholed", nodeId, null)

/**
 * A replicating node timed out while waiting for a response to a write request to one of the
 * replica nodes. This is a "future" timeout and indicates that the replication future timeout
 * is lower than your per-database write timeout. It only occurs when doing parallel writes.
 */
class ReplicationTimeoutException(nodeId: RoutingNodeId, ex: Throwable)
  extends NormalRoutingException("Timeout waiting for write to node", nodeId, ex)
