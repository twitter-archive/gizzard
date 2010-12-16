package com.twitter.gizzard.thrift

import java.util.{List => JList}
import conversions.Sequences._
import scheduler._

class JobInjectorService(
  codecParam: JsonCodec,
  scheduler: PrioritizingJobScheduler[JsonJob])
extends JobInjector.Iface {

  private val codec = codecParam match {
    case c: ReplicatingJsonCodec => c.innerCodec
    case _ => codecParam
  }

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

    override def toJson = {
      if (isDeserialized) {
        deserialized.toJson
      } else {
        new String(serialized, "UTF-8")
      }
    }
  }

  def inject_jobs(jobs: JList[thrift.Job]) {
    jobs.toSeq.foreach { j =>
      var job: JsonJob = new InjectedJsonJob(j.getContents())
      if (j.is_replicated) job = new ReplicatedJob(List(job))

      scheduler.put(j.priority, job)
    }
  }
}
