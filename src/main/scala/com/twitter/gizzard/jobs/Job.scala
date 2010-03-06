package com.twitter.gizzard.jobs

import scala.reflect.Manifest
import com.twitter.json.Json
import com.twitter.ostrich.{StatsProvider, W3CStats}
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import proxy.LoggingProxy
import shards.ShardRejectedOperationException


trait Schedulable {
  def toMap: Map[String, Any]
  def className = getClass.getName
  def toJson: String = Json.build(Map(className -> toMap)).toString
  def loggingName = className.lastIndexOf('.') match {
    case -1 => className
    case n => className.substring(n + 1)
  }
}

class SchedulableProxy(schedulable: Schedulable) extends Schedulable {
  def toMap = schedulable.toMap
  override def className = schedulable.className
  override def loggingName = schedulable.loggingName
}

trait Job extends Schedulable {
  @throws(classOf[Exception])
  def apply()
}

abstract class JobProxy(job: Job) extends SchedulableProxy(job) with Job

trait UnboundJob[E] extends Schedulable {
  @throws(classOf[Exception])
  def apply(environment: E)
}

case class BoundJob[E](protected val unboundJob: UnboundJob[E], protected val environment: E)(implicit manifest: Manifest[E]) extends SchedulableProxy(unboundJob) with Job {
  def apply() { unboundJob(environment) }
}

class LoggingJob(w3cStats: W3CStats, job: Job) extends JobProxy(job) {
  def apply() { LoggingProxy(w3cStats, job.loggingName, job).apply() }
}

case class ErrorHandlingConfig(maxErrorCount: Int, badJobsLogger: String => Unit, stats: Option[StatsProvider])

class ErrorHandlingJob(job: Job, errorQueue: MessageQueue, config: ErrorHandlingConfig, var errorCount: Int) extends JobProxy(job) {
  private val log = Logger.get(getClass.getName)

  def apply() {
    try {
      job()
      config.stats.map { _.incr("job-success-count", 1) }
    } catch {
      case e: ShardRejectedOperationException =>
        config.stats.map { _.incr("job-darkmoded-count", 1) }
        errorQueue.put(this)
      case e =>
        config.stats.map { _.incr("job-error-count", 1) }
        log.error(e, "Error in Job: " + e)
        errorCount += 1
        if (errorCount > config.maxErrorCount) {
          config.badJobsLogger(toJson)
        } else {
          errorQueue.put(this)
        }
    }
  }

  override def toMap = job.toMap ++ Map("error_count" -> errorCount)

  override def toString = "ErrorHandlingJob(%s, %s, %d)".format(job, errorQueue, errorCount)
}

case class ScheduleableWithTasks(tasks: Iterable[Schedulable]) extends Schedulable {
  def toMap = Map("tasks" -> tasks.map { task => (Map(task.className -> task.toMap)) })
  override def className = classOf[JobWithTasks].getName

  override def equals(that: Any) = that match {
    case that: ScheduleableWithTasks => tasks.toList == that.tasks.toList
    case _ => false
  }
}

case class JobWithTasks(override val tasks: Iterable[Job]) extends ScheduleableWithTasks(tasks) with Job {
  def apply() {
    for (task <- tasks) task()
  }

  override def loggingName = tasks.map(_.loggingName).mkString(",")
}

class JournaledJob(job: Job, journaller: String => Unit) extends JobProxy(job) {
  def apply() {
    job()
    try {
      journaller(job.toJson)
    } catch {
      case e: Exception =>
        val log = Logger.get(getClass.getName)
        log.warning(e, "Failed to journal job: %s", job.toJson)
    }
  }
}
