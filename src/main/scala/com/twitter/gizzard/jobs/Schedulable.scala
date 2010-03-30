package com.twitter.gizzard.jobs

import com.twitter.json.Json


trait Schedulable {
  def toMap: Map[String, Any]
  def className = getClass.getName
  def toJson = Json.build(Map(className -> toMap)).toString
  def loggingName = className.lastIndexOf('.') match {
    case -1 => className
    case n => className.substring(n + 1)
  }
}

abstract class SchedulableProxy(schedulable: Schedulable) extends Schedulable {
  def toMap = schedulable.toMap
  override def className = schedulable.className
  override def loggingName = schedulable.loggingName
}