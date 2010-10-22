package com.twitter.gizzard.config

import com.twitter.util.Duration


trait ThreadPool {
  def name: String
  def stopTimeout: Int = 60
  def minThreads: Int
  def maxThreads: Int = Math.MAX_INT
}

trait TSelectorServer {
  def port: Int
  def clientTimeout: Duration
  def idleTimeout: Duration
  def threadPool: ThreadPool
}
