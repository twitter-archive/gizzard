package com.twitter.gizzard.util

trait Process {
  def start()
  def pause()
  def resume()
  def shutdown()
  def isShutdown: Boolean
}
