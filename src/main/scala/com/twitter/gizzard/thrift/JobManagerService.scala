package com.twitter.gizzard.thrift

import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.sharding._


class JobManagerService(scheduler: jobs.PrioritizingScheduler) extends JobManager.Iface {
  def retry_errors() {
    scheduler.retryErrors()
  }

  def stop_writes() {
    scheduler.pauseWork()
  }

  def resume_writes() {
    scheduler.resumeWork()
  }

  def retry_errors_for(priority: Int) {
    scheduler(priority).retryErrors()
  }

  def stop_writes_for(priority: Int) {
    scheduler(priority).pauseWork()
  }

  def resume_writes_for(priority: Int) {
    scheduler(priority).resumeWork()
  }

  def is_writing(priority: Int) = !scheduler(priority).isShutdown

  def inject_job(priority: Int, job: String) {
    scheduler(priority)(new jobs.Schedulable {
      override def toJson = job
      def toMap = null
    })
  }
}
