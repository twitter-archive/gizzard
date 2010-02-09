package com.twitter.gizzard.sharding

import java.sql.{ResultSet, SQLIntegrityConstraintViolationException}
import scala.collection.mutable
import com.twitter.db.QueryEvaluator
import net.lag.configgy.Configgy
import net.lag.logging.Logger


object NameServer {
  private val config = Configgy.config
  private val log = Logger.get(getClass.getName)

  class NonExistentShard extends Exception("Shard does not exist")
  class InvalidShard extends Exception("Shard has invalid attributes (such as hostname)")

  case class ChildShard(shardId: Int, weight: Int)

  @volatile protected var shardInfos = mutable.Map.empty[Int, ShardInfo]
  @volatile private var familyTree = mutable.Map.empty[Int, mutable.ArrayBuffer[ChildShard]]

  def reload(queryEvaluator: QueryEvaluator) {
    val newShardInfos = mutable.Map.empty[Int, ShardInfo]
    queryEvaluator.select("SELECT * FROM shards") { result =>
      val shardInfo = new ShardInfo(result.getString("class_name"), result.getString("table_prefix"),
        result.getString("hostname"), result.getString("source_type"), result.getString("destination_type"),
        Busy.fromThrift(result.getInt("busy")), result.getInt("id"))
      newShardInfos += (result.getInt("id") -> shardInfo)
    }
    shardInfos = newShardInfos

    val newFamilyTree = new mutable.HashMap[Int, mutable.ArrayBuffer[ChildShard]] { override def initialSize = shardInfos.size * 10 }
    queryEvaluator.select(config("nameserver.query_timeout").toInt, "SELECT * FROM shard_children ORDER BY parent_id, position ASC") { result =>
      val children = newFamilyTree.getOrElseUpdate(result.getInt("parent_id"), new mutable.ArrayBuffer[ChildShard])
      children += ChildShard(result.getInt("child_id"), result.getInt("weight"))
    }
    familyTree = newFamilyTree
  }

  def getShardInfo(id: Int) = shardInfos(id)
}

abstract class NameServer[Key, S <: Shard] {
  def create(key: Key, shardId: Int)
  def find(key: Key): S

  protected val queryEvaluator: QueryEvaluator
  protected val shardRepository: ShardRepository[S]

  private def rowToShardInfo(row: ResultSet) = {
    new ShardInfo(row.getString("class_name"), row.getString("table_prefix"), row.getString("hostname"),
      row.getString("source_type"), row.getString("destination_type"), Busy.fromThrift(row.getInt("busy")),
      row.getInt("id"))
  }

  def createShard(shardInfo: ShardInfo) = {
    queryEvaluator.transaction { transaction =>
      try {
        val shardId = transaction.selectOne("SELECT id, class_name, source_type, destination_type FROM shards WHERE table_prefix = ? AND hostname = ?", shardInfo.tablePrefix, shardInfo.hostname) { row =>
          if (row.getString("class_name") != shardInfo.className || row.getString("source_type") != shardInfo.sourceType || row.getString("destination_type") != shardInfo.destinationType) {
            throw new NameServer.InvalidShard
          }
          row.getInt("id")
        } getOrElse {
          transaction.insert("INSERT INTO shards (class_name, table_prefix, hostname, source_type, destination_type) VALUES (?, ?, ?, ?, ?)", shardInfo.className, shardInfo.tablePrefix, shardInfo.hostname, shardInfo.sourceType, shardInfo.destinationType)
        }
        shardRepository.create(shardInfo)
        shardId
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          throw new NameServer.InvalidShard
      }
    }
  }

  def findShard(shardInfo: ShardInfo): Int = {
    queryEvaluator.selectOne("SELECT id FROM shards WHERE table_prefix = ? AND hostname = ?", shardInfo.tablePrefix, shardInfo.hostname) { row =>
      row.getInt("id")
    } getOrElse {
      throw new NameServer.NonExistentShard
    }
  }

  def getShard(shardId: Int): ShardInfo = {
    queryEvaluator.selectOne("SELECT * FROM shards WHERE id = ?", shardId) { row =>
      rowToShardInfo(row)
    } getOrElse {
      throw new NameServer.NonExistentShard
    }
  }

  def updateShard(shardInfo: ShardInfo) {
    val rows = queryEvaluator.execute(
      "UPDATE shards SET class_name = ?, table_prefix = ?, hostname = ?, source_type = ?, " +
      "destination_type = ? WHERE id = ?",
      shardInfo.className, shardInfo.tablePrefix, shardInfo.hostname, shardInfo.sourceType,
      shardInfo.destinationType, shardInfo.shardId)
    if (rows < 1) {
      throw new NameServer.NonExistentShard
    }
  }

  def deleteShard(shardId: Int) {
    queryEvaluator.execute("DELETE FROM shard_children where parent_id = ? OR child_id = ?", shardId, shardId)
    if (queryEvaluator.execute("DELETE FROM shards where id = ?", shardId) == 0) {
      throw new NameServer.NonExistentShard
    }
  }

  def addChildShard(parentShardId: Int, childShardId: Int, position: Int, weight: Int) {
    queryEvaluator.execute("INSERT INTO shard_children (parent_id, child_id, position, weight) VALUES (?, ?, ?, ?)",
      parentShardId, childShardId, position, weight)
  }

  def removeChildShard(parentShardId: Int, childShardId: Int) {
    if (queryEvaluator.execute("DELETE FROM shard_children WHERE parent_id = ? AND child_id = ?", parentShardId, childShardId) == 0) {
      throw new NameServer.NonExistentShard
    }
  }

  def replaceChildShard(oldChildShardId: Int, newChildShardId: Int) {
    queryEvaluator.execute("UPDATE shard_children SET child_id = ? WHERE child_id = ?", newChildShardId, oldChildShardId)
  }

  def listShardChildren(shardId: Int) = {
    queryEvaluator.select("SELECT child_id, position, weight FROM shard_children WHERE parent_id = ? ORDER BY position ASC", shardId) { row =>
      new ChildInfo(row.getInt("child_id"), row.getInt("position"), row.getInt("weight"))
    }.toList
  }

  def markShardBusy(shardId: Int, busy: Busy.Value) {
    if (queryEvaluator.execute("UPDATE shards SET busy = ? WHERE id = ?", busy.id, shardId) == 0) {
      throw new NameServer.NonExistentShard
    }
  }

  def shardIdsForHostname(hostname: String, className: String): List[Int] = {
    val shardIds = new mutable.ListBuffer[Int]
    queryEvaluator.select("SELECT id FROM shards WHERE hostname = ? AND class_name = ?", hostname, className) { row =>
      row.getInt("id")
    }.toList
  }

  def shardsForHostname(hostname: String, className: String): List[ShardInfo] = {
    val shardIds = new mutable.ListBuffer[Int]
    queryEvaluator.select("SELECT * FROM shards WHERE hostname = ? AND class_name = ?", hostname, className) { row =>
      rowToShardInfo(row)
    }.toList
  }

  def getBusyShards() = {
    queryEvaluator.select("SELECT * FROM shards where busy != 0") { row => rowToShardInfo(row) }.toList
  }

  def getParentShard(shardId: Int): ShardInfo = {
    queryEvaluator.select("SELECT parent_id FROM shard_children WHERE child_id = ?", shardId) { row =>
      row.getInt("parent_id")
    }.firstOption match {
      case None => getShard(shardId)
      case Some(parentId) => getShard(parentId)
    }
  }

  def getRootShard(shardId: Int): ShardInfo = {
    queryEvaluator.select("SELECT parent_id FROM shard_children WHERE child_id = ?", shardId) { row =>
      row.getInt("parent_id")
    }.firstOption match {
      case None => getShard(shardId)
      case Some(parentId) => getRootShard(parentId)
    }
  }

  def getChildShardsOfClass(parentShardId: Int, className: String): List[ShardInfo] = {
    val childIds = listShardChildren(parentShardId)
    childIds.map { child => getShard(child.shardId) }.filter { _.className == className }.toList ++
      childIds.flatMap { child => getChildShardsOfClass(child.shardId, className) }
  }

  def reload() {
    NameServer.reload(queryEvaluator)
    reloadForwardings()
  }

  def findShardById(shardId: Int, weight: Int): S = {
    val shardInfo = NameServer.shardInfos.getOrElse(shardId, throw new NameServer.NonExistentShard)
    val children =
      NameServer.familyTree.getOrElse(shardId, new mutable.ArrayBuffer[NameServer.ChildShard]).map { child =>
        findShardById(child.shardId, child.weight)
      }.toList
    shardRepository.find(shardInfo, weight, children)
  }

  def findShardById(shardId: Int): S = findShardById(shardId, 1)

  def replaceForwarding(oldShardId: Int, newShardId: Int)

  def setForwarding(forwarding: Forwarding)

  def getForwarding(tableId: List[Int], baseId: Long): ShardInfo

  def getForwardingForShard(shardId: Int): Forwarding

  def getForwardings(): List[Forwarding]

  protected def reloadForwardings()

  def findCurrentForwarding(tableId: List[Int], id: Long): ShardInfo
}
