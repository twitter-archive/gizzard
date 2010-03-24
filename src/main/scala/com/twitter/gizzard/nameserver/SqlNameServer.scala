package com.twitter.gizzard.nameserver

import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import com.twitter.querulous.evaluator.QueryEvaluator
import scheduler.JobScheduler
import scala.collection.mutable
import shards._


object SqlNameServer {
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
    weight                  INT NOT NULL DEFAULT 1,

    UNIQUE unique_family (parent_id, child_id),
    UNIQUE unique_child (child_id)
) ENGINE=INNODB
/* ALTER TABLE shard_children ADD weight INT NOT NULL DEFAULT 1; */
"""

  val FORWARDINGS_DDL = """
CREATE TABLE forwardings (
    service_id              INT                     NOT NULL,
    base_source_id          BIGINT                  NOT NULL,
    table_id                INT                     NOT NULL,
    shard_id                INT                     NOT NULL,

    PRIMARY KEY (service_id, base_source_id, table_id),

    UNIQUE unique_shard_id (shard_id)
) ENGINE=INNODB;
"""

  val SEQUENCE_DDL = """
CREATE TABLE IF NOT EXISTS sequences (
    service_id              INT                     NOT NULL,
    id                      INT UNSIGNED            NOT NULL,

    PRIMARY KEY (service_id)
) ENGINE=INNODB;
"""
}

class SqlNameServer(queryEvaluator: QueryEvaluator, shardMaterializerFactory: ShardInfo => Unit)
                           extends ManagingNameServer {
  val children = List()
  val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.SqlNameServer", "", "")
  val weight = 1 // hardcode for now

  private def rowToShardInfo(row: ResultSet) = {
    new ShardInfo(row.getString("class_name"), row.getString("table_prefix"), row.getString("hostname"),
      row.getString("source_type"), row.getString("destination_type"), Busy(row.getInt("busy")),
      row.getInt("id"))
  }

  private def rowToForwarding(row: ResultSet) = {
    new Forwarding(row.getInt("service_id"), row.getInt("table_id"), row.getLong("base_source_id"), row.getInt("shard_id"))
  }

  private def rowToChildInfo(row: ResultSet) = {
    new ChildInfo(row.getInt("child_id"), row.getInt("weight"))
  }

  def createShard(shardInfo: ShardInfo) = {
    queryEvaluator.transaction { transaction =>
      try {
        val shardId = transaction.selectOne("SELECT id, class_name, source_type, destination_type FROM shards WHERE table_prefix = ? AND hostname = ?", shardInfo.tablePrefix, shardInfo.hostname) { row =>
          if (row.getString("class_name") != shardInfo.className || row.getString("source_type") != shardInfo.sourceType || row.getString("destination_type") != shardInfo.destinationType) {
            throw new InvalidShard
          }
          row.getInt("id")
        } getOrElse {
          transaction.insert("INSERT INTO shards (class_name, table_prefix, hostname, source_type, destination_type) VALUES (?, ?, ?, ?, ?)", shardInfo.className, shardInfo.tablePrefix, shardInfo.hostname, shardInfo.sourceType, shardInfo.destinationType)
        }
        shardMaterializerFactory(shardInfo)
        shardId
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          throw new InvalidShard
      }
    }
  }

  def findShard(shardInfo: ShardInfo): Int = {
    queryEvaluator.selectOne("SELECT id FROM shards WHERE table_prefix = ? AND hostname = ?", shardInfo.tablePrefix, shardInfo.hostname) { row =>
      row.getInt("id")
    } getOrElse {
      throw new NonExistentShard
    }
  }

  def getShard(shardId: Int): ShardInfo = {
    queryEvaluator.selectOne("SELECT * FROM shards WHERE id = ?", shardId) { row =>
      rowToShardInfo(row)
    } getOrElse {
      throw new NonExistentShard
    }
  }

  def updateShard(shardInfo: ShardInfo) {
    val rows = queryEvaluator.execute(
      "UPDATE shards SET class_name = ?, table_prefix = ?, hostname = ?, source_type = ?, " +
      "destination_type = ? WHERE id = ?",
      shardInfo.className, shardInfo.tablePrefix, shardInfo.hostname, shardInfo.sourceType,
      shardInfo.destinationType, shardInfo.shardId)
    if (rows < 1) {
      throw new NonExistentShard
    }
  }

  def deleteShard(shardId: Int) {
    queryEvaluator.execute("DELETE FROM shard_children where parent_id = ? OR child_id = ?", shardId, shardId)
    if (queryEvaluator.execute("DELETE FROM shards where id = ?", shardId) == 0) {
      throw new NonExistentShard
    }
  }

  def addChildShard(parentShardId: Int, childShardId: Int, weight: Int) {
    queryEvaluator.execute("INSERT INTO shard_children (parent_id, child_id, weight) VALUES (?, ?, ?)",
      parentShardId, childShardId, weight)
  }

  def removeChildShard(parentShardId: Int, childShardId: Int) {
    if (queryEvaluator.execute("DELETE FROM shard_children WHERE parent_id = ? AND child_id = ?", parentShardId, childShardId) == 0) {
      throw new NonExistentShard
    }
  }

  def replaceChildShard(oldChildShardId: Int, newChildShardId: Int) {
    queryEvaluator.execute("UPDATE shard_children SET child_id = ? WHERE child_id = ?", newChildShardId, oldChildShardId)
  }

  def listShardChildren() = {
    val map = mutable.HashMap.empty[Int, mutable.ArrayBuffer[ChildInfo]]
    queryEvaluator.select("SELECT parent_id, child_id, weight FROM shard_children ORDER BY parent_id, child_id") { row =>
      val arrayBuffer = map.getOrElseUpdate(row.getInt("parent_id"), new mutable.ArrayBuffer[ChildInfo]())
      arrayBuffer += rowToChildInfo(row)
    }
    map
  }

  def listShardChildren(shardId: Int) = {
    queryEvaluator.select("SELECT child_id, weight FROM shard_children WHERE parent_id = ? ORDER BY weight DESC", shardId) { row =>
      rowToChildInfo(row)
    }.toList
  }

  def listShards() = {
    queryEvaluator.select("SELECT * FROM shards") { row =>
      rowToShardInfo(row)
    }.toList
  }

  def markShardBusy(shardId: Int, busy: Busy.Value) {
    if (queryEvaluator.execute("UPDATE shards SET busy = ? WHERE id = ?", busy.id, shardId) == 0) {
      throw new NonExistentShard
    }
  }

  def setForwarding(forwarding: Forwarding) {
    if (queryEvaluator.execute("UPDATE forwardings SET shard_id = ? WHERE base_source_id = ? AND table_id = ? AND service_id = ?",
                               forwarding.shardId, forwarding.baseId, forwarding.tableId, forwarding.serviceId) == 0) {
      queryEvaluator.execute("INSERT INTO forwardings (service_id, base_source_id, table_id, shard_id) VALUES (?, ?, ?, ?)",
                             forwarding.serviceId, forwarding.baseId, forwarding.tableId, forwarding.shardId)
    }
  }

  def replaceForwarding(oldShardId: Int, newShardId: Int) {
    queryEvaluator.execute("UPDATE forwardings SET shard_id = ? WHERE shard_id = ?", newShardId, oldShardId)
  }

  def getForwarding(serviceId: Int, tableId: Int, baseId: Long): ShardInfo = {
    getShard(queryEvaluator.select("SELECT shard_id FROM forwardings WHERE service_id = ? AND base_source_id = ? AND table_id = ?", serviceId, baseId, tableId) { row =>
      row.getInt("shard_id")
    }.firstOption.getOrElse { throw new ShardException("No such forwarding") })
  }

  def getForwardingForShard(shardId: Int): Forwarding = {
    queryEvaluator.select("SELECT * FROM forwardings WHERE shard_id = ?", shardId) { row =>
      rowToForwarding(row)
    }.firstOption.getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardings(): List[Forwarding] = {
    // XXX: REVISIT ORDERING
    queryEvaluator.select("SELECT * FROM forwardings ORDER BY service_id, table_id, base_source_id DESC") { row =>
      rowToForwarding(row)
    }.toList
  }


  def shardIdsForHostname(hostname: String, className: String): List[Int] = {
    queryEvaluator.select("SELECT id FROM shards WHERE hostname = ? AND class_name = ?", hostname, className) { row =>
      row.getInt("id")
    }.toList
  }

  def shardsForHostname(hostname: String, className: String): List[ShardInfo] = {
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

/*  def reload() {
    try {
      List("shards", "shard_children", forwardingTable, tablePrefix + "_sequence").foreach { table =>
        queryEvaluator.select("DESCRIBE " + table) { row => }
      }
    } catch {
      case e: SQLException =>
        // try creating the schema
        rebuildSchema()
    }

    SqlNameServer.reload(queryEvaluator)
    reloadForwardings()
  } */

  def rebuildSchema() {
    queryEvaluator.execute("DROP TABLE IF EXISTS shards")
    queryEvaluator.execute("DROP TABLE IF EXISTS shard_children")
    queryEvaluator.execute(SqlNameServer.SHARDS_DDL)
    queryEvaluator.execute(SqlNameServer.SHARD_CHILDREN_DDL)
    queryEvaluator.execute("DROP TABLE IF EXISTS forwardings")
    queryEvaluator.execute("DROP TABLE IF EXISTS sequences")
    queryEvaluator.execute(SqlNameServer.FORWARDINGS_DDL)
    queryEvaluator.execute(SqlNameServer.SEQUENCE_DDL)
//    queryEvaluator.execute("INSERT INTO_sequences VALUES (0)")
  }
}
