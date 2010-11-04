package com.twitter.gizzard

trait Process {
  def start()
  def pause()
  def resume()
  def shutdown()
  def isShutdown: Boolean
}
