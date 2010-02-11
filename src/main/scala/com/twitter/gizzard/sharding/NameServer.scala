package com.twitter.gizzard.sharding

import java.sql.{ResultSet, SQLIntegrityConstraintViolationException}
import scala.collection.mutable
import com.twitter.querulous.evaluator.QueryEvaluator
import net.lag.logging.Logger


object NameServer {
  val SHARDS_DDL = """
CREATE TABLE shards (
    id                      INT          NOT NULL AUTO_INCREMENT,
    class_name              VARCHAR(125) NOT NULL,
    table_prefix            VARCHAR(125) NOT NULL,
    hostname                VARCHAR(25)  NOT NULL,
    source_type             VARCHAR(125),
    destination_type        VARCHAR(125),
    busy                    TINYINT      NOT NULL DEFAULT 0,

    PRIMARY KEY primary_key_id (id),

    UNIQUE unique_name (table_prefix, hostname)
) ENGINE=INNODB
"""

  val SHARD_CHILDREN_DDL = """
CREATE TABLE shard_children (
    parent_id               INT NOT NULL,
    child_id                INT NOT NULL,
    position                INT NOT NULL,
    weight                  INT NOT NULL DEFAULT 1,

    UNIQUE unique_family (parent_id, child_id),
    UNIQUE unique_child (child_id)
) ENGINE=INNODB
/* ALTER TABLE shard_children ADD weight INT NOT NULL DEFAULT 1; */
"""

  private val log = Logger.get(getClass.getName)

  class NonExistentShard extends Exception("Shard does not exist")
  class InvalidShard extends Exception("Shard has invalid attributes (such as hostname)")

  @volatile protected var shardInfos = mutable.Map.empty[Int, ShardInfo]
  @volatile private var familyTree = mutable.Map.empty[Int, mutable.ArrayBuffer[ChildInfo]]

  def reload(queryEvaluator: QueryEvaluator) {
    val newShardInfos = mutable.Map.empty[Int, ShardInfo]
    queryEvaluator.select("SELECT * FROM shards") { result =>
      val shardInfo = new ShardInfo(result.getString("class_name"), result.getString("table_prefix"),
        result.getString("hostname"), result.getString("source_type"), result.getString("destination_type"),
        Busy.fromThrift(result.getInt("busy")), result.getInt("id"))
      newShardInfos += (result.getInt("id") -> shardInfo)
    }
    shardInfos = newShardInfos

    val newFamilyTree = new mutable.HashMap[Int, mutable.ArrayBuffer[ChildInfo]] { override def initialSize = shardInfos.size * 10 }
    queryEvaluator.select("SELECT * FROM shard_children ORDER BY parent_id, position ASC") { result =>
      val children = newFamilyTree.getOrElseUpdate(result.getInt("parent_id"), new mutable.ArrayBuffer[ChildInfo])
      children += ChildInfo(result.getInt("child_id"), result.getInt("position"), result.getInt("weight"))
    }
    familyTree = newFamilyTree
  }

  def getShardInfo(id: Int) = shardInfos(id)

  def rebuildSchema(queryEvaluator: QueryEvaluator) {
    queryEvaluator.execute("DROP TABLE IF EXISTS shards")
    queryEvaluator.execute("DROP TABLE IF EXISTS shard_children")
    queryEvaluator.execute(SHARDS_DDL)
    queryEvaluator.execute(SHARD_CHILDREN_DDL)
  }
}

class NameServer[S <: Shard](queryEvaluator: QueryEvaluator, shardRepository: ShardRepository[S], forwardingManager: ForwardingManager[S]) {
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

  def copyShard(sourceShardId: Int, destinationShardId: Int) {
    // FIXME
  }

  def setupMigration(sourceShardInfo: ShardInfo, destinationShardInfo: ShardInfo) = {
    val lastDot = sourceShardInfo.className.lastIndexOf('.')
    val packageName = if (lastDot >= 0) sourceShardInfo.className.substring(0, lastDot + 1) else ""
    val sourceShardId = findShard(sourceShardInfo)
    val destinationShardId = createShard(destinationShardInfo)

    val writeOnlyShard = new ShardInfo(packageName + "WriteOnlyShard",
      sourceShardInfo.tablePrefix + "_migrate_write_only", "localhost", "", "", Busy.Normal, 0)
    val writeOnlyShardId = createShard(writeOnlyShard)
    addChildShard(writeOnlyShardId, destinationShardId, 1, 1)

    val replicatingShard = new ShardInfo(packageName + "ReplicatingShard",
      sourceShardInfo.tablePrefix + "_migrate_replicating", "localhost", "", "", Busy.Normal, 0)
    val replicatingShardId = createShard(replicatingShard)
    replaceChildShard(sourceShardId, replicatingShardId)
    addChildShard(replicatingShardId, sourceShardId, 1, 1)
    addChildShard(replicatingShardId, writeOnlyShardId, 2, 1)

    forwardingManager.replaceForwarding(sourceShardId, replicatingShardId)
    new ShardMigration(sourceShardId, destinationShardId, replicatingShardId, writeOnlyShardId)
  }

  def migrateShard(migration: ShardMigration) {
    // FIXME
  }

  def setForwarding(forwarding: Forwarding) {
    forwardingManager.setForwarding(forwarding)
  }

  def replaceForwarding(oldShardId: Int, newShardId: Int) {
    forwardingManager.replaceForwarding(oldShardId, newShardId)
  }

  def getForwarding(tableId: List[Int], baseId: Long): ShardInfo = {
    getShard(forwardingManager.getForwarding(tableId, baseId))
  }

  def getForwardingForShard(shardId: Int): Forwarding = {
    forwardingManager.getForwardingForShard(shardId)
  }

  def getForwardings(): List[Forwarding] = {
    forwardingManager.getForwardings()
  }

  def findCurrentForwarding(tableId: List[Int], id: Long): S = {
    forwardingManager.findCurrentForwarding(tableId, id)
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
    forwardingManager.reloadForwardings(this)
  }

  def findShardById(shardId: Int, weight: Int): S = {
    val shardInfo = NameServer.shardInfos.getOrElse(shardId, throw new NameServer.NonExistentShard)
    val children =
      NameServer.familyTree.getOrElse(shardId, new mutable.ArrayBuffer[ChildInfo]).map { child =>
        findShardById(child.shardId, child.weight)
      }.toList
    shardRepository.find(shardInfo, weight, children)
  }

  def findShardById(shardId: Int): S = findShardById(shardId, 1)
}
