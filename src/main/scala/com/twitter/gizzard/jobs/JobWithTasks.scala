package com.twitter.gizzard.jobs


class JobWithTasksParser(jobParser: JobParser) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = {
    val (jobClassName, attributes) = json.toList.first
    attributes.get("tasks").map { taskJsons =>
      val tasks = taskJsons.asInstanceOf[Iterable[Map[String, Map[String, Any]]]].map(this(_))
      val jobClass = Class.forName(jobClassName).asInstanceOf[Class[Job]]
      jobClass.getConstructor(classOf[Iterable[Job]]).newInstance(tasks)
    } getOrElse {
      jobParser(json)
    }
  }
}

case class JobWithTasks(tasks: Iterable[Job]) extends SchedulableWithTasks(tasks) with Job {
  def apply() = for (task <- tasks) task()
}

