package com.twitter.gizzard

import java.util.concurrent._
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.{Duration, Time}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap


class Future(name: String, poolSize: Int, maxPoolSize: Int, keepAlive: Duration,
             val timeout: Duration) {

  def this(name: String, config: ConfigMap) =
    this(name, config("pool_size").toInt, config("max_pool_size").toInt,
         config("keep_alive_time_seconds").toInt.seconds,
         config("timeout_seconds").toInt.seconds)

  var executor = new ThreadPoolExecutor(poolSize, maxPoolSize, keepAlive.inSeconds,
    TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable], new NamedPoolThreadFactory(name))

  Stats.makeGauge("future-" + name + "-queue-size") { executor.getQueue().size() }

  def apply[A](a: => A) = {
    val future = new FutureTask(new Callable[A] {
      val startTime = Time.now
      def call = {
        if (Time.now - startTime > timeout) {
          Stats.incr("future-" + name + "-timeout")
          throw new TimeoutException("future spent too long in queue")
        }
        a
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

class ParallelSeq[A](seq: Seq[A], future: Future) extends Seq[A] {
  def length = seq.length

  def apply(i: Int) = seq(i)

  def elements = seq.elements

  override def map[B](f: A => B) = {
    seq.map { a => future(f(a)) }.map { _.get(future.timeout.inMillis, TimeUnit.MILLISECONDS) }
  }

  override def flatMap[B](f: A => Iterable[B]) = {
    seq.map { a => future(f(a)) }.flatMap { _.get(future.timeout.inMillis, TimeUnit.MILLISECONDS) }
  }
}
