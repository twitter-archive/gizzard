package com.twitter.gizzard.jobs

import com.twitter.json.Json


class SchedulableWithTasks(protected val tasks: Iterable[Schedulable]) extends Schedulable {
  override def loggingName = tasks.map(_.loggingName).mkString(",")
  def toMap = Map("tasks" -> tasks.map { task => (Map(task.className -> task.toMap)) })
  override def equals(other: Any) = {
    other match {
      case other: SchedulableWithTasks =>
        tasks.toList == other.tasks.toList
      case _ =>
        false
    }
  }
}