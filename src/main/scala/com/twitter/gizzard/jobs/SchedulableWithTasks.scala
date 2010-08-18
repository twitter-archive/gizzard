package com.twitter.gizzard.jobs

import com.twitter.json.Json


class SchedulableWithTasks(protected val tasks: Iterable[Schedulable]) extends Schedulable {
  override def loggingName = tasks.map(_.loggingName).mkString(",")

  def remainingTasks = tasks

  def toMap = Map("tasks" -> remainingTasks.map { task => (Map(task.className -> task.toMap)) })
  override def equals(other: Any) = {
    other match {
      case other: SchedulableWithTasks =>
        remainingTasks.toList == other.remainingTasks.toList
      case _ =>
        false
    }
  }
}
