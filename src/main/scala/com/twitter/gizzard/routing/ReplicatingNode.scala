package com.twitter.gizzard.routing

import java.lang.reflect.UndeclaredThrowableException
import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import net.lag.logging.Logger


class ReplicatingNode(val id: RoutingNodeId,
                      val weight: Int,
                      val children: Seq[RoutingNode],
                      val loadBalancer: (() => Seq[RoutingNode]),
                      val future: Option[Future]) extends RoutingNode {

  def readOperation(op: ReadOperation): OperationResult = failover(_.readOperation(op), children)

  def readAllOperation(op: ReadOperation): Seq[OperationResult] = fanout(_.readAllOperation(op), children)

  def rebuildableReadOperation(op: RebuildableReadOperation, state: RebuildState): RebuildState =
    rebuildableFailover(_.rebuildableReadOperation(op, _), state, children)

  def writeOperation(op: WriteOperation): OperationResult = {
    fanout(_.writeOperation(op)::Nil, children).map {
      case ex: ExceptionResult => return ex  // Return the first failure, if any.
      case answer: AnswerResult => answer
    }.firstOption.getOrElse(throw new NodeBlackHoleException(id))
  }

  lazy val log = Logger.get

  protected def unwrapException(exception: Throwable): Throwable = {
    exception match {
      case e: ExecutionException => unwrapException(e.getCause)
      case e: UndeclaredThrowableException =>  unwrapException(e.getCause) // fondly known as JavaOutrageException
      case e => e
    }
  }

  protected def fanoutFuture(method: RoutingNode => Seq[OperationResult], replicas: Seq[RoutingNode], future: Future) = {
    val results = new mutable.ArrayBuffer[OperationResult]()

    replicas.map { replica => (replica, future(method(replica))) }.foreach { case (replica, futureTask) =>
      try {
        results ++= futureTask.get(future.timeout.inMillis, TimeUnit.MILLISECONDS)
      } catch {
        case e: Exception =>
          unwrapException(e) match {
            case e: NodeBlackHoleException =>  // nothing.
            case e: TimeoutException => results += ExceptionResult(new ReplicationTimeoutException(replica.id, e))
            case e => results += ExceptionResult(e)
          }
      }
    }

    results
  }

  protected def fanoutSerial(method: RoutingNode => Seq[OperationResult], replicas: Seq[RoutingNode]) = {
    val results = new mutable.ArrayBuffer[OperationResult]()

    replicas.foreach { replica =>
      try {
        results ++= method(replica)
      } catch {
        case e: NodeBlackHoleException =>  // nothing.
        case e: TimeoutException => results += ExceptionResult(new ReplicationTimeoutException(replica.id, e))
        case e => results += ExceptionResult(e)
      }
    }

    results
  }

  protected def fanout(method: RoutingNode => Seq[OperationResult], replicas: Seq[RoutingNode]): Seq[OperationResult] = {
    future match {
      case None => fanoutSerial(method, replicas)
      case Some(f) => fanoutFuture(method, replicas, f)
    }
  }

  protected def failover(method: RoutingNode => OperationResult, replicas: Seq[RoutingNode]): OperationResult = {
    replicas match {
      case Seq() =>
        throw new NodeOfflineException(id)
      case Seq(replica, remainder @ _*) =>
        try {
          method(replica)
        } catch {
          case e: NodeRejectedOperationException =>
            failover(method, remainder)
          case e: RoutingException =>
            log.warning(e, "Error on %s: %s", replica.id, e)
            failover(method, remainder)
        }
      }
  }

  protected def rebuildableFailover(method: (RoutingNode, RebuildState) => RebuildState,
                                    state: RebuildState,
                                    replicas: Seq[RoutingNode]): RebuildState = {
    replicas match {
      case Seq() =>
        if (state.everSuccessful) {
          state
        } else {
          throw new NodeOfflineException(id)
        }
      case Seq(replica, remainder @ _*) =>
        try {
          val newState = method(replica, state)
          newState.result match {
            case None =>
              rebuildableFailover(method, new RebuildState(None, replica :: newState.toRebuild, true), remainder)
            case Some(answer) => newState
          }
        } catch {
          case e: NodeRejectedOperationException =>
            rebuildableFailover(method, state, remainder)
          case e: RoutingException =>
            log.warning(e, "Error on %s: %s", id, e)
            rebuildableFailover(method, state, remainder)
        }
    }
  }
}
