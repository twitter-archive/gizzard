package com.twitter.gizzard.routing

trait RoutableOperation {
}

trait ReadOperation extends RoutableOperation {
}

trait WriteOperation extends RoutableOperation {
}

trait RebuildableReadOperation extends RoutableOperation {
}

trait OperationResult {
}

case class ExceptionResult(ex: Throwable) extends OperationResult {
}

case class AnswerResult[A](answer: A) extends OperationResult {
}

case class RoutingNodeId(id: String) {
}

case class RebuildState(val result: Option[OperationResult],
                        val toRebuild: List[RoutingNode],
                        everSuccessful: Boolean)

trait RoutingNode {
  def id: RoutingNodeId
  def weight: Int
  def children: Seq[RoutingNode]

  def readOperation(op: ReadOperation): OperationResult
  def readAllOperation(op: ReadOperation): Seq[OperationResult]
  def rebuildableReadOperation(op: RebuildableReadOperation, state: RebuildState): RebuildState

  def writeOperation(op: WriteOperation): OperationResult
}


