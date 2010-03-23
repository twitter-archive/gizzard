package com.twitter.gizzard.scheduler

import jobs.{Schedulable, Job, JobParser}


class JobQueue(messageQueue: Scheduler[String], jobParser: JobParser) extends Scheduler[Schedulable] {
  def put(schedulable: Schedulable) = messageQueue.put(schedulable.toJson)
}