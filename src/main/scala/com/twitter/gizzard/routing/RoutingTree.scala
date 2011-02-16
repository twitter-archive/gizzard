package com.twitter.gizzard.routing

// The client-facing entry point to the gizzard routing tree. Adapts the client-facing API to
// the RoutingNode internal API (usually trivially).
class RoutingTree(val root: RoutingNode) {

  def readOperation(op: ReadOperation): OperationResult = root.readOperation(op)

  def readAllOperation(op: ReadOperation): Seq[OperationResult] = root.readAllOperation(op)

  def rebuildableReadOperation(op: RebuildableReadOperation): OperationResult = {
    def finalState = root.rebuildableReadOperation(op, new RebuildState(None, Nil, false))
    finalState.result.getOrElse(throw new NodeOfflineException(root.id))
  }

  def writeOperation(op: WriteOperation): OperationResult = root.writeOperation(op)
}
