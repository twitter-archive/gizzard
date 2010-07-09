package com.twitter.gizzard.nameserver

import com.twitter.querulous.evaluator.QueryEvaluator

class NameServer(queryEvaluator: QueryEvaluator) extends SqlShard(queryEvaluator) with shards.LinkSource with shards.ShardInfoSource
