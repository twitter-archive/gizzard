package com.twitter.gizzard

import scala.collection.SeqProxy
import scala.collection.generic.CanBuildFrom
import java.util.concurrent._
import com.twitter.util.{Duration, Time}
import com.twitter.conversions.time._

class Future(name: String, poolSize: Int, maxPoolSize: Int, keepAlive: Duration,
             val timeout: Duration) {

  var executor = new ThreadPoolExecutor(poolSize, maxPoolSize, keepAlive.inSeconds,
    TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable], new NamedPoolThreadFactory(name))

  Stats.addGauge("future-" + name + "-queue-size") { executor.getQueue().size() }

  def apply[A](a: => A) = {
    val trans = Stats.transactionOpt.map { _.createChild }

    val future = new FutureTask(new Callable[A] {
      val startTime = Time.now
      def call = {
        trans.foreach { t => Stats.setTransaction(t) }
        val timeInQueue = Time.now - startTime
        Stats.transaction.record("Time spent in future queue: "+timeInQueue.inMillis)
        if (timeInQueue > timeout) {
          Stats.incr("future-" + name + "-timeout")
          throw new TimeoutException("future spent too long in queue")
        }

        val threadExecTime = Time.now
        try {
          a
        } catch {
          case e: Exception =>
            Stats.transaction.record("Caught exception: "+e)
            throw e
        } finally {
          val duration = Time.now - threadExecTime
          Stats.transaction.record("Total duration: "+duration.inMillis)
          trans.foreach { t => Stats.endTransaction() }
        }
      }
    })
    executor.execute(future)
    future
  }

  def shutdown() {
    executor.shutdown()
    executor.awaitTermination(60, TimeUnit.SECONDS)
  }
}

class ParallelSeq[A](seq: Seq[A], future: Future) extends SeqProxy[A] {
  def self = seq

  override def map[B, That](f: A => B)(implicit bf: CanBuildFrom[Seq[A], B, That]): That = {
    val coll: Seq[B] = if (seq.size <= 1) {
      seq.map(f)
    } else {
      seq.map { a => future(f(a)) }.map { _.get(future.timeout.inMillis, TimeUnit.MILLISECONDS) }
    }

    val b = bf(repr)
    for (x <- coll) b += x
    b.sizeHint(coll)
    b.result
  }

  override def flatMap[B, That](f: A => Traversable[B])(implicit bf: CanBuildFrom[Seq[A], B, That]): That = {
    val coll: Seq[B] = if (seq.size <= 1) {
      seq.flatMap(f)
    } else {
      seq.map { a => future(f(a)) }.flatMap { _.get(future.timeout.inMillis, TimeUnit.MILLISECONDS) }
    }

    val b = bf(repr)
    for (x <- coll) b += x
    b.sizeHint(coll)
    b.result
  }
}
