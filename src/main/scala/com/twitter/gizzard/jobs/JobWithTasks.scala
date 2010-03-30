package com.twitter.gizzard.jobs


class JobWithTasksParser(jobParser: JobParser) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = {
    val (_, attributes) = json.toList.first
    attributes.get("tasks").map { taskJsons =>
      val tasks = taskJsons.asInstanceOf[Iterable[Map[String, Map[String, Any]]]].map(this(_))
      new JobWithTasks(tasks)
    } getOrElse {
      jobParser(json)
    }
  }
}

case class JobWithTasks(override val tasks: Iterable[Job]) extends SchedulableWithTasks(tasks) with Job {
  def apply() = tasks.foreach(_.apply())
}

