package com.twitter.gizzard.scheduler.cursor

import com.twitter.gizzard.shards._

trait CursorJobFactory[S <: Shard] extends ((Source[S], DestinationList[S]) => CursorJob[S])