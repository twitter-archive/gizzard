package com.twitter.gizzard.nameserver

import com.twitter.gizzard.thrift.{TransformOperation => ThriftTransformOperation}
import com.twitter.gizzard.thrift.TransformOperation._Fields._
import com.twitter.gizzard.thrift.conversions.Forwarding._
import com.twitter.gizzard.thrift.conversions.ShardId._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.shards.{ShardId, ShardInfo}
import com.twitter.util.CompactThriftSerializer

sealed abstract class TransformOperation
case class CreateShard(shardInfo : ShardInfo) extends TransformOperation
case class DeleteShard(shardId : ShardId) extends TransformOperation
case class AddLink(upId : ShardId, downId : ShardId, weight : Int) extends TransformOperation
case class RemoveLink(upId : ShardId, downId : ShardId) extends TransformOperation
case class SetForwarding(forwarding : Forwarding) extends TransformOperation
case class RemoveForwarding(forwarding : Forwarding) extends TransformOperation
case object Commit extends TransformOperation

object TransformOperation {
  def apply(thriftCommand : ThriftTransformOperation) : TransformOperation = {
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
      case COMMIT => Commit
    }
  }

  def serialize(thriftCommand: ThriftTransformOperation): Array[Byte] =
    new CompactThriftSerializer().toBytes(thriftCommand)

  def deserialize(buffer: Array[Byte]): ThriftTransformOperation = {
    val thriftCommand = new ThriftTransformOperation()
    new CompactThriftSerializer().fromBytes(thriftCommand, buffer)
    thriftCommand
  }
}
