package com.twitter.gizzard.jobs
import scala.collection.mutable.Queue

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
  private val taskQueue = {
    val q = new Queue[Job]()
    q ++= tasks
    q
  }

  override val remainingTasks = taskQueue

  def apply() = {
    while (!taskQueue.isEmpty) {
      taskQueue.first.apply()
      taskQueue.dequeue()
    }
  }
}
