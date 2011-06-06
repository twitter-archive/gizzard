package com.twitter.gizzard.shards

import java.util.concurrent.Executors
import scala.collection.generic.CanBuild
import com.twitter.util.Duration
import com.twitter.util.FuturePool
import com.twitter.util.{Try, Throw}
import com.twitter.conversions.time._

class ParConfig(val pool: FuturePool, val timeout: Duration)

object ParConfig {
  lazy val default = new ParConfig(FuturePool(Executors.newCachedThreadPool()), 99.days)
}

trait ParNodeIterable[T] extends NodeIterable[T] {
  protected def futurePool: FuturePool
  protected def timeout: Duration

  override def all[R, That](f: T => R)(implicit bf: CanBuild[Try[R], That] = Seq.canBuildFrom[Try[R]]): That = {
    val futures = activeShards map { case (_, s) => futurePool(f(s)) }

    val b = bf()
    for (future <- futures)  b += Try(future.get(timeout).apply())
    for (s <- blockedShards) b += Throw(new ShardOfflineException(s.id))
    b.result
  }
}

class ParNodeSet[T](
  root: ShardInfo,
  active: Seq[(ShardInfo, T)],
  blocked: Seq[ShardInfo],
  protected val futurePool: FuturePool,
  protected val timeout: Duration)
extends NodeSet[T](root, active, blocked)
with ParNodeIterable[T]
