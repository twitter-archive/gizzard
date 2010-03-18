package com.twitter.gizzard.jobs

import com.twitter.json.Json


class SchedulableWithTasks(tasks: Iterable[Schedulable]) extends Schedulable {
  override def loggingName = tasks.map(_.loggingName).mkString(",")
  def toMap = Map("tasks" -> tasks.map { task => (Map(task.className -> task.toMap)) })
}