package com.twitter.gizzard.scheduler

class RemoteReplicatingJob[J <: JsonJob](jobs: Iterable[J], var replicated: Boolean) extends JsonNestedJob(jobs) {
  override def toMap = {
    super.toMap ++ Map("replicated" -> replicated)
  }

  override def apply() {
    // Forward job here
    replicated = true
    super.apply()
  }
}

class RemoteReplicatingJobParser[J <: JsonJob](codec: JsonCodec[J]) extends JsonNestedJobParser(codec) {
  def apply(json: Map[String, Any]) = {
    val replicated = json("replicated").asInstanceOf[Boolean]
    val nestedJob = super.apply(json)

    new RemoteReplicatingJob(nestedJob.jobs, replicated)
  }
}
