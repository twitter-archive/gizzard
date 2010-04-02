package com.twitter.gizzard.nameserver

import jobs.Copy


trait Copier[S <: shards.Shard] extends ((Int, Int) => Copy[S])