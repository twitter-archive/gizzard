package com.twitter.gizzard.scheduler

trait Job {
  type Environment
  val environment: Environment

  @throws(classOf[Exception])
  def apply(environment: Environment): Unit

  var errorCount: Int = 0
  var errorMessage: String = "(none)"

  @throws(classOf[Exception])
  def apply(): Unit = apply(environment)

  def loggingName = {
    val className = getClass.getName
    className.lastIndexOf('.') match {
      case -1 => className
      case n => className.substring(n + 1)
    }
  }
}

/**
 * A wrapper or proxy for a Job. A JobProxy has no implicit environment and overrides the
 * default apply() method instead of implementing apply(environment).
 */
trait JobProxy extends Job {
  type Environment = Unit
  val environment = ()

  def apply(environment: Environment) {
    // does nothing.
  }
}
