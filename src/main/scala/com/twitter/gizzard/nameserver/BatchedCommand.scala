package com.twitter.gizzard.nameserver

import com.twitter.gizzard.thrift.{BatchedCommand => ThriftBatchedCommand}
import com.twitter.gizzard.thrift.BatchedCommand._Fields._
import com.twitter.gizzard.thrift.conversions.Forwarding._
import com.twitter.gizzard.thrift.conversions.ShardId._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.shards.{ShardId, ShardInfo}

sealed abstract class BatchedCommand
case class CreateShard(shardInfo : ShardInfo) extends BatchedCommand
case class DeleteShard(shardId : ShardId) extends BatchedCommand
case class AddLink(upId : ShardId, downId : ShardId, weight : Int) extends BatchedCommand
case class RemoveLink(upId : ShardId, downId : ShardId) extends BatchedCommand
case class SetForwarding(forwarding : Forwarding) extends BatchedCommand
case class RemoveForwarding(forwarding : Forwarding) extends BatchedCommand

object BatchedCommand {
  def apply(thriftCommand : ThriftBatchedCommand) : BatchedCommand = {
    thriftCommand.getSetField() match {
      case CREATE_SHARD => CreateShard(thriftCommand.getCreate_shard().fromThrift)
      case DELETE_SHARD => DeleteShard(thriftCommand.getDelete_shard().fromThrift)
      case ADD_LINK => {
        val addLink = thriftCommand.getAdd_link()
        AddLink(addLink.getUp_id().fromThrift, addLink.getDown_id().fromThrift, addLink.getWeight())
      }
      case REMOVE_LINK => {
        val removeLink = thriftCommand.getRemove_link()
        RemoveLink(removeLink.getUp_id().fromThrift, removeLink.getDown_id().fromThrift)
      }
      case SET_FORWARDING => SetForwarding(thriftCommand.getSet_forwarding().fromThrift)
      case REMOVE_FORWARDING => RemoveForwarding(thriftCommand.getRemove_forwarding().fromThrift)
    }
  }
}
