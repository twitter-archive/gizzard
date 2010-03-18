package com.twitter.gizzard.fake


class JobParser extends jobs.JobParser {
  def apply(json: Map[String, Map[String, Any]]) = {
    val (_, attributes) = json.toList.first
    new jobs.BoundJob(new Job(attributes), 1)
  }
}
