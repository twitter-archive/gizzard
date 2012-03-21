package com.twitter.gizzard.nameserver

import com.twitter.gizzard.thrift.{TransformCommand => ThriftTransformCommand}
import com.twitter.gizzard.thrift.TransformCommand._Fields._
import com.twitter.gizzard.thrift.conversions.Forwarding._
import com.twitter.gizzard.thrift.conversions.ShardId._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.shards.{ShardId, ShardInfo}

sealed abstract class TransformCommand
case class CreateShard(shardInfo : ShardInfo) extends TransformCommand
case class DeleteShard(shardId : ShardId) extends TransformCommand
case class AddLink(upId : ShardId, downId : ShardId, weight : Int) extends TransformCommand
case class RemoveLink(upId : ShardId, downId : ShardId) extends TransformCommand
case class SetForwarding(forwarding : Forwarding) extends TransformCommand
case class RemoveForwarding(forwarding : Forwarding) extends TransformCommand

object TransformCommand {
  def apply(thriftCommand : ThriftTransformCommand) : TransformCommand = {
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
