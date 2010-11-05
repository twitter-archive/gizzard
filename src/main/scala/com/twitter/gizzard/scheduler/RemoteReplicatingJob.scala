package com.twitter.gizzard.scheduler

class RemoteReplicatingJob[J <: JsonJob](jobs: Iterable[J], var replicated: Boolean) extends JsonNestedJob(jobs) {
  override def toMap: Map[String, Any] = Map("replicated" -> replicated :: super.toMap.toList: _*)

  override def apply() {
    // Forward job here
    replicated = true
    super.apply()
  }
}

class RemoteReplicatingJobParser[J <: JsonJob](codec: JsonCodec[J]) extends JsonNestedJobParser(codec) {
  override def apply(json: Map[String, Any]) = {
    val replicated = json("replicated").asInstanceOf[Boolean]
    val nestedJob = super.apply(json).asInstanceOf[JsonNestedJob[J]]

    new RemoteReplicatingJob(nestedJob.jobs, replicated).asInstanceOf[J]
  }
}
