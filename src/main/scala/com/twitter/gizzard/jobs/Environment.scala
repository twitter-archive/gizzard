package com.twitter.gizzard.jobs
import scheduler.JobScheduler

case class Environment[ConcreteShard <: shards.Shard](val nameServer: nameserver.Shard, val root: ConcreteShard, val scheduler: JobScheduler)