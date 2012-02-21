package com.twitter.gizzard.thrift

import java.nio.ByteBuffer
import com.twitter.util.Future
import com.twitter.gizzard.thrift.{Job => ThriftJob}
import com.twitter.gizzard.scheduler._

class JobInjectorService(
  val serverName: String,
  val thriftPort: Int,
  codecParam: JsonCodec,
  scheduler: PrioritizingJobScheduler)
extends JobInjector.ThriftServer {

  private val codec = codecParam.innerCodec

  private class InjectedJsonJob(serialized: Array[Byte]) extends JsonJob {
    private var isDeserialized = false
    private lazy val deserialized = {
      isDeserialized = true
      codec.inflate(serialized)
    }

    override def className   = deserialized.className
    override def loggingName = deserialized.loggingName

    def apply = deserialized.apply()
    def toMap = deserialized.toMap

    override def toJsonBytes = {
      if (isDeserialized) {
        deserialized.toJsonBytes
      } else {
        serialized
      }
    }
  }

  def injectJobs(jobs: Seq[ThriftJob]) = {
    jobs foreach { j =>
      val bytes = new Array[Byte](j.contents.remaining)
      j.contents.get(bytes)

      var job: JsonJob = new InjectedJsonJob(bytes)
      if (j.isReplicated getOrElse false) job = new ReplicatedJob(List(job))

      scheduler.put(j.priority, job)
    }

    Future.Done
  }
}
