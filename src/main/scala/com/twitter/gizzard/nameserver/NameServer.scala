package com.twitter.gizzard.nameserver

import shards._


class NonExistentShard extends ShardException("Shard does not exist")
class InvalidShard extends ShardException("Shard has invalid attributes (such as hostname)")
