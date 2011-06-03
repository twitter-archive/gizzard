package com.twitter.gizzard.shards

import scala.annotation.tailrec
import scala.collection.generic.CanBuild
import com.twitter.util.{Try, Throw, Future}

// For read or write, three node states:
// - normal: should apply normally
// - block:  should indicate error
// - skip:   should ignore the shard.

// skip(ShardId).read.any(T => R) => R

// read.blockedShards => Seq[ShardInfo]
// read.iterator => Iterator[T]
// read.map(T => R) => Seq[R]      // throws error if block exists.
// read.foreach(T => R) => Seq[R]  // throws error if block exists.
// read.all(T => R) => Seq[Try[R]]
// read.any(T => R) => R
// write.all(T => R) => Seq[Try[R]]

// iterator => Iterator[(T, ShardInfo)]
// map((T, ShardInfo) => R) => Seq[R]
// all((T, ShardInfo) => R) => Seq[Try[R]]

// withInfo -> RoutingNode[(T, ShardInfo)]


trait NodeIterable[T] {
  def rootInfo: ShardInfo
  def activeShards: Seq[(ShardInfo, T)]
  def blockedShards: Seq[ShardInfo]

  def containsBlocked = !blockedShards.isEmpty

  def anyOption[R](f: T => R): Option[R] = _any(iterator, s => Try(f(s)) ).toOption

  def tryAny[R](f: T => Try[R]): Try[R] = _any(iterator, f)

  def any[R](f: T => R): R = {
    if (activeShards.isEmpty && blockedShards.isEmpty) {
      throw new ShardBlackHoleException(rootInfo.id)
    }

    _any(iterator, s => Try(f(s)) ).apply()
  }

  @tailrec protected final def _any[R](iter: Iterator[T], f: T => Try[R]): Try[R] = {
    if (iter.hasNext) {
      f(iter.next) match {
        case rv if rv.isReturn => rv
        case _ => _any(iter, f)
      }
    } else {
      Throw(new ShardOfflineException(rootInfo.id))
    }
  }

  // XXX: it would be nice to have a way to implement all in terms of fmap. :(
  def fmap[R, That](f: T => Future[R])(implicit bf: CanBuild[Future[R], That] = Seq.canBuildFrom[Future[R]]): That = {
    val b = bf()
    for ((i, s) <- activeShards) b += f(s)
    for (s <- blockedShards)     b += Future.exception(new ShardOfflineException(s.id))
    b.result
  }

  def all[R, That](f: T => R)(implicit bf: CanBuild[Try[R], That] = Seq.canBuildFrom[Try[R]]): That = {
    tryAll { s => Try(f(s)) }
  }

  def tryAll[R, That](f: T => Try[R])(implicit bf: CanBuild[Try[R], That] = Seq.canBuildFrom[Try[R]]): That = {
    val b = bf()
    for ((i, s) <- activeShards) b += f(s)
    for (s <- blockedShards)     b += Throw(new ShardOfflineException(s.id))
    b.result
  }

  // iterators are lazy, so map works here.
  def iterator: Iterator[T] = activeShards.iterator map { case (i, s) => s }

  // throws error if block exists
  def map[R, That](f: T => R)(implicit bf: CanBuild[R, That]): That = {
    val b = bf()
    for (s <- this) b += f(s)
    b.result
  }

  // throws error if block exists
  def flatMap[R, That](f: T => Traversable[R])(implicit bf: CanBuild[R, That]): That = {
    val b = bf()
    for (s <- this) b ++= f(s)
    b.result
  }

  def foreach[U](f: T => U) {
    all(f) foreach {
      case Throw(e) => throw e
      case _        => ()
    }
  }
}

class NodeSet[T](
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

  def skip(ss: ShardId*) = {
    val set = ss.toSet
    filterNot { (info, _) => set contains info.id }
  }
}
