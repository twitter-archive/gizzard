package com.twitter.gizzard.shards

trait Cursorable[T] extends Ordered[T] {
  def atEnd: Boolean
  def atStart: Boolean
}

