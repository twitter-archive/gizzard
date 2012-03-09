package com.twitter.gizzard.scheduler

class ReplicatedJob(jobs: Iterable[JsonJob]) extends JsonNestedJob(jobs) {
  override val shouldReplicate = false
}

class ReplicatedJobParser(codec: JsonCodec) extends JsonJobParser {
  type Tasks = Iterable[Map[String, Any]]

  override def apply(json: Map[String, Any]) = {
    val tasks = json("tasks").asInstanceOf[Tasks].map(codec.inflate)
    new ReplicatedJob(tasks)
  }
}
