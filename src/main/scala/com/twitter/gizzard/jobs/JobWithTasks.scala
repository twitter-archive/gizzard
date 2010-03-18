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

case class JobWithTasks(tasks: Iterable[Job]) extends Job {
  def apply() = for (task <- tasks) task()
  def toMap = Map("tasks" -> tasks.map { task => (Map(task.className -> task.toMap)) })
  override def className = classOf[JobWithTasks].getName
  override def loggingName = tasks.map(_.loggingName).mkString(",")
}