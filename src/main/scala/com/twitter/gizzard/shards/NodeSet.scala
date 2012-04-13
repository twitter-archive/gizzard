package com.twitter.gizzard.shards

import java.util.concurrent.CancellationException
import scala.annotation.tailrec
import scala.collection.generic.CanBuild
import com.twitter.util.{Try, Return, Throw, Future, Promise}

// For read or write, three node states:
// - normal: should apply normally
// - block:  should indicate error
// - skip:   should ignore the shard.

// skip(ShardId).read.any(T => R) => R

// read.blockedShards => Seq[ShardInfo]
// read.iterator => Iterator[(ShardId, T)]
// read.map(T => R) => Seq[R]      // throws error if block exists.
// read.foreach(T => R) => Seq[R]  // throws error if block exists.
// read.all(T => R) => Seq[Try[R]]
// read.any(T => R) => R
// write.all(T => R) => Seq[Try[R]]

// iterator => Iterator[(T, ShardInfo)]
// map((T, ShardInfo) => R) => Seq[R]
// all((T, ShardInfo) => R) => Seq[Try[R]]

// withInfo -> RoutingNode[(T, ShardInfo)]


trait NodeIterable[+T] {
  def rootInfo: ShardInfo
  def activeShards: Seq[(ShardInfo, T)]
  def blockedShards: Seq[ShardInfo]

  def lookupId[U >: T](u: U) = activeShards find { case (i, t) => t == u } map { _._1 }

  def size = activeShards.size + blockedShards.size

  def length = size

  def containsBlocked = !blockedShards.isEmpty

  // iterators are lazy, so map works here.
  def iterator: Iterator[(ShardId, T)] = activeShards.iterator map { case (i, s) => (i.id, s) }


  def anyOption[R](f: (ShardId, T) => R): Option[R] = {
    // only wrap ShardExceptions in Throw. Allow others to raise naturally
    tryAny { (i, s) => try { Return(f(i, s)) } catch { case e: ShardException => Throw(e) } }.toOption
  }

  def anyOption[R](f: T => R): Option[R] = {
    this anyOption { (_, t) => f(t) }
  }


  def any[R](f: (ShardId, T) => R): R = {
    // only wrap ShardExceptions in Throw. Allow others to raise naturally
    tryAny { (i, s) => try { Return(f(i, s)) } catch { case e: ShardException => Throw(e) } }.apply()
  }

  def any[R](f: T => R): R = {
    this any { (_, t) => f(t) }
  }


  def tryAny[R](f: (ShardId, T) => Try[R]): Try[R] = {
    if (activeShards.isEmpty && blockedShards.isEmpty) {
      Throw(new ShardBlackHoleException(rootInfo.id))
    } else {
      _any(iterator, f)
    }
  }

  def tryAny[R](f: T => Try[R]): Try[R] = {
    this tryAny { (_, t) => f(t) }
  }

  @tailrec
  protected final def _any[T1 >: T, R](
    iter: Iterator[(ShardId, T1)],
    f: (ShardId, T1) => Try[R]
  ): Try[R] = {

    if (iter.hasNext) {
      val (id, s) = iter.next

      f(id, s) match {
        case rv if rv.isReturn => rv
        case _ => _any(iter, f)
      }
    } else {
      Throw(new ShardOfflineException(rootInfo.id))
    }
  }

  def futureAny[R](f: (ShardId, T) => Future[R]): Future[R] = {
    if (activeShards.isEmpty && blockedShards.isEmpty) {
      Future.exception(new ShardBlackHoleException(rootInfo.id))
    } else {
      _futureAny(iterator, f)
    }
  }

  def futureAny[R](f: T => Future[R]): Future[R] = {
    this futureAny { (_, t) => f(t) }
  }

  protected final def _futureAny[T1 >: T, R](
      iter: Iterator[(ShardId, T1)],
      f: (ShardId, T1) => Future[R]): Future[R] = {
    if (iter.hasNext) {
      val (id, s) = iter.next
      f(id, s) rescue {
        // No point in continuing to iterate if the exception was due to
        // Future cancellation.
        case e: CancellationException => Future.exception(e)
        case _ => _futureAny(iter, f)
      }
    } else {
      Future.exception(new ShardOfflineException(rootInfo.id))
    }
  }


  // XXX: it would be nice to have a way to implement all in terms of fmap. :(
  def fmap[R, That](f: (ShardId, T) => Future[R])(implicit bf: CanBuild[Future[R], That]): That = {
    val b = bf()
    for ((i, s) <- activeShards) b += f(i.id, s)
    for (i <- blockedShards)     b += Future.exception(new ShardOfflineException(i.id))
    b.result
  }

  def fmap[R, That](f: T => Future[R])(implicit bf: CanBuild[Future[R], That]): That = {
    this fmap { (_, t) => f(t) }
  }


  def all[R, That](f: (ShardId, T) => R)(implicit bf: CanBuild[Try[R], That]): That = {
    this tryAll { (i, s) => Try(f(i, s)) }
  }

  def all[R, That](f: T => R)(implicit bf: CanBuild[Try[R], That]): That = {
    this all { (_, t) => f(t) }
  }


  def tryAll[R, That](f: (ShardId, T) => Try[R])(implicit bf: CanBuild[Try[R], That]): That = {
    val b = bf()
    for ((i, s) <- activeShards) b += f(i.id, s)
    for (i <- blockedShards)     b += Throw(new ShardOfflineException(i.id))
    b.result
  }

  def tryAll[R, That](f: T => Try[R])(implicit bf: CanBuild[Try[R], That]): That = {
    this tryAll { (_, t) => f(t) }
  }


  // throws error if block exists
  def map[R, That](f: (ShardId, T) => R)(implicit bf: CanBuild[R, That]): That = {
    val b = bf()
    this foreach { (i, s) => b += f(i, s) }
    b.result
  }

  def map[R, That](f: T => R)(implicit bf: CanBuild[R, That]): That = {
    this map { (_, t) => f(t) }
  }


  // throws error if block exists
  def flatMap[R, That](f: (ShardId, T) => Traversable[R])(implicit bf: CanBuild[R, That]): That = {
    val b = bf()
    this foreach { (i, s) => b ++= f(i, s) }
    b.result
  }

  def flatMap[R, That](f: T => Traversable[R])(implicit bf: CanBuild[R, That]): That = {
    this flatMap { (_, t) => f(t) }
  }


  def foreach[U](f: (ShardId, T) => U) {
    all(f) foreach {
      case Throw(e) => throw e
      case _        => ()
    }
  }

  def foreach[U](f: T => U) {
    this foreach { (_, t) => f(t) }
  }

  override def toString() =
    "<NodeIterable(%s)>".format(rootInfo)
}

class NodeSet[+T](
  val rootInfo: ShardInfo, // XXX: replace with forwarding id.
  val activeShards: Seq[(ShardInfo, T)],
  val blockedShards: Seq[ShardInfo])
extends NodeIterable[T] {

  def par(implicit cfg: ParConfig = ParConfig.default): NodeSet[T] = {
    new ParNodeSet(rootInfo, activeShards, blockedShards, cfg.pool, cfg.timeout)
  }

  def filter(f: (ShardInfo, Option[T]) => Boolean) = {
    val activeFiltered  = activeShards filter { case (i, s) => f(i, Some(s)) }
    val blockedFiltered = blockedShards filter { i => f(i, None) }
    new NodeSet(rootInfo, activeFiltered, blockedFiltered)
  }

  def filterNot(f: (ShardInfo, Option[T]) => Boolean) = filter { (i, s) => !f(i, s) }

  def skip(ss: Seq[ShardId]) = {
    val set = ss.toSet
    filterNot { (info, _) => set contains info.id }
  }

  def skip(s: ShardId, ss: ShardId*): NodeSet[T] = skip(s +: ss)
}
