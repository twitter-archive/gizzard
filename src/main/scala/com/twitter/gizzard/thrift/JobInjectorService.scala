package com.twitter.gizzard.thrift

import java.util.{List => JList}
import conversions.Sequences._
import scheduler.{JsonJob, JsonCodec, PrioritizingJobScheduler}

class JobInjectorService[J <: JsonJob](codec: JsonCodec[J], scheduler: PrioritizingJobScheduler[J]) extends JobInjector.Iface {
  private class InjectedJsonJob[J <: JsonJob](serialized: Array[Byte]) extends JsonJob {
    private var isDeserialized = false
    private lazy val deserialized = {
      isDeserialized = true
      codec.inflate(serialized).asInstanceOf[J]
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
      scheduler.put(j.priority, new InjectedJsonJob(j.contents).asInstanceOf[J])
    }
  }
}
