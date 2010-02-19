package com.twitter.gizzard.shards


trait ReadWriteShard[ConcreteShard <: Shard] extends Shard {
  protected def readOperation[A](method: (ConcreteShard => A)): A
  protected def writeOperation[A](method: (ConcreteShard => A)): A
}
