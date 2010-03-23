package com.twitter.gizzard.scheduler

import scala.collection.mutable
import jobs.Schedulable


trait Scheduler[A] {
  def put(value: A)
}
