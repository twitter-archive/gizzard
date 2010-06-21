package com.twitter.gizzard.nameserver

import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import scala.collection.mutable
import com.twitter.querulous.evaluator.QueryEvaluator
import scheduler.JobScheduler
import shards._


object SqlShard {
  val SHARDS_DDL = """
CREATE TABLE IF NOT EXISTS shards (
    class_name              VARCHAR(125) NOT NULL,
    table_prefix            VARCHAR(125) NOT NULL,
    hostname                VARCHAR(25)  NOT NULL,
    source_type             VARCHAR(125),
    destination_type        VARCHAR(125),
    busy                    TINYINT      NOT NULL DEFAULT 0,

   PRIMARY KEY primary_key_table_prefix_hostname (hostname, table_prefix)
) ENGINE=INNODB
"""

  val SHARD_CHILDREN_DDL = """
CREATE TABLE IF NOT EXISTS shard_children (
    parent_hostname         VARCHAR(125) NOT NULL,
    parent_table_prefix     VARCHAR(125) NOT NULL,
    child_hostname          VARCHAR(125) NOT NULL,
    child_table_prefix      VARCHAR(125) NOT NULL,
    weight                  INT NOT NULL DEFAULT 1,

    PRIMARY KEY primary_key_family (parent_hostname, parent_table_prefix, child_hostname, child_table_prefix),
    INDEX child (child_hostname, child_table_prefix)
) ENGINE=INNODB
"""

  val FORWARDINGS_DDL = """
CREATE TABLE IF NOT EXISTS forwardings (
    base_source_id          BIGINT                  NOT NULL,
    table_id                INT                     NOT NULL,
    shard_hostname          VARCHAR(125)            NOT NULL,
    shard_table_prefix      VARCHAR(125)            NOT NULL,

    PRIMARY KEY (base_source_id, table_id),

    UNIQUE unique_shard (shard_hostname, shard_table_prefix)
) ENGINE=INNODB;
"""
}


class SqlShard(queryEvaluator: QueryEvaluator) extends Shard {
  val children = List()
  val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.SqlShard", "", "")
  val weight = 1 // hardcode for now

  private def rowToShardInfo(row: ResultSet) = {
    ShardInfo(ShardId(row.getString("hostname"), row.getString("table_prefix")), row.getString("class_name"),
      row.getString("source_type"), row.getString("destination_type"), Busy(row.getInt("busy")))
  }

  private def rowToForwarding(row: ResultSet) = {
    Forwarding(row.getInt("table_id"), row.getLong("base_source_id"), ShardId(row.getString("shard_hostname"), row.getString("shard_table_prefix")))
  }

  private def rowToLinkInfo(row: ResultSet) = {
    LinkInfo(ShardId(row.getString("parent_hostname"), row.getString("parent_table_prefix")),
             ShardId(row.getString("child_hostname"), row.getString("child_table_prefix")),
             row.getInt("weight"))
  }

  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]) {
    queryEvaluator.transaction { transaction =>
      try {
        transaction.selectOne("SELECT class_name, source_type, destination_type " +
                              "FROM shards WHERE table_prefix = ? AND hostname = ?",
                              shardInfo.tablePrefix, shardInfo.hostname) { row =>
          if (row.getString("class_name") != shardInfo.className ||
              row.getString("source_type") != shardInfo.sourceType ||
              row.getString("destination_type") != shardInfo.destinationType) {
            throw new InvalidShard
          }
        } getOrElse {
          transaction.insert("INSERT INTO shards (hostname, table_prefix, class_name, " +
                             "source_type, destination_type) VALUES (?, ?, ?, ?, ?)",
                             shardInfo.hostname, shardInfo.tablePrefix, shardInfo.className,
                             shardInfo.sourceType, shardInfo.destinationType)
          repository.create(shardInfo)
        }
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          throw new InvalidShard
      }
    }
  }

  def getShard(id: ShardId) = {
    queryEvaluator.selectOne("SELECT * FROM shards WHERE hostname = ? AND table_prefix = ?", id.hostname, id.tablePrefix) { row =>
      rowToShardInfo(row)
    } getOrElse {
      throw new NonExistentShard
    }
  }

/*  def updateShard(shardInfo: ShardInfo) {
    val rows = queryEvaluator.execute(
      "UPDATE shards SET class_name = ?, source_type = ?, " +
      "destination_type = ? WHERE hostname = ? AND table_prefix = ?",
      shardInfo.className, shardInfo.sourceType, shardInfo.destinationType, shardInfo.hostname,
      shardInfo.tablePrefix)
    if (rows < 1) {
      throw new NonExistentShard
    }
  } */

  def deleteShard(id: ShardId) {
    queryEvaluator.execute("DELETE FROM shard_children WHERE "+
                           "(parent_hostname = ? AND parent_table_prefix = ?) OR " +
                           "(child_hostname = ? AND child_table_prefix = ?)",
                           id.hostname, id.tablePrefix, id.hostname, id.tablePrefix)
    queryEvaluator.execute("DELETE FROM shards WHERE hostname = ? AND table_prefix = ?", id.hostname, id.tablePrefix) == 0
  }

  def addLink(upId: ShardId, downId: ShardId, weight: Int) {
    queryEvaluator.execute("INSERT INTO shard_children (parent_hostname, parent_table_prefix, child_hostname, child_table_prefix, weight) VALUES (?, ?, ?, ?, ?)",
      upId.hostname, upId.tablePrefix, downId.hostname, downId.tablePrefix, weight)
    // XXX: todo - check loops
  }

  def removeLink(upId: ShardId, downId: ShardId) {
    queryEvaluator.execute("DELETE FROM shard_children WHERE parent_hostname = ? AND parent_table_prefix = ? AND child_hostname = ? AND child_table_prefix = ?",
      upId.hostname, upId.tablePrefix, downId.hostname, downId.tablePrefix) == 0
  }

  def listLinks() = {
    queryEvaluator.select("SELECT * FROM shard_children ORDER BY parent_hostname, parent_table_prefix") { row =>
      rowToLinkInfo(row)
    }.toList
  }

  def listDownwardLinks(id: ShardId) = {
    queryEvaluator.select("SELECT * FROM shard_children WHERE parent_hostname = ? AND parent_table_prefix = ? ORDER BY weight DESC", id.hostname, id.tablePrefix) { row =>
      rowToLinkInfo(row)
    }.toList
  }

  def listUpwardLinks(id: ShardId) = {
    queryEvaluator.select("SELECT * FROM shard_children WHERE child_hostname = ? AND child_table_prefix = ? ORDER BY weight DESC", id.hostname, id.tablePrefix) { row =>
      rowToLinkInfo(row)
    }.toList
  }

  def listShards() = {
    queryEvaluator.select("SELECT * FROM shards") { row =>
      rowToShardInfo(row)
    }.toList
  }

  def markShardBusy(id: ShardId, busy: Busy.Value) {
    if (queryEvaluator.execute("UPDATE shards SET busy = ? WHERE hostname = ? AND table_prefix = ?", busy.id, id.hostname, id.tablePrefix) == 0) {
      throw new NonExistentShard
    }
  }

  def setForwarding(forwarding: Forwarding) {
    queryEvaluator.execute("REPLACE forwardings (base_source_id, table_id, shard_hostname, shard_table_prefix) VALUES (?, ?, ?, ?)",
      forwarding.baseId, forwarding.tableId, forwarding.shardId.hostname, forwarding.shardId.tablePrefix)
  }

  def replaceForwarding(oldId: ShardId, newId: ShardId) {
    queryEvaluator.execute("UPDATE forwardings SET shard_hostname = ?, shard_table_prefix = ? WHERE shard_hostname = ? AND shard_table_prefix = ?",
      newId.hostname, newId.tablePrefix, oldId.hostname, oldId.tablePrefix)
  }

  def getForwarding(tableId: Int, baseId: Long) = {
    queryEvaluator.selectOne("SELECT * FROM forwardings WHERE base_source_id = ? AND table_id = ?", baseId, tableId) { row =>
      rowToForwarding(row)
    } getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardingForShard(id: ShardId) = {
    queryEvaluator.select("SELECT * FROM forwardings WHERE shard_hostname = ? AND shard_table_prefix = ?", id.hostname, id.tablePrefix) { row =>
      rowToForwarding(row)
    }.firstOption.getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardings() = {
    queryEvaluator.select("SELECT * FROM forwardings ORDER BY table_id, base_source_id ASC") { row =>
      rowToForwarding(row)
    }.toList
  }

  def shardsForHostname(hostname: String) = {
    queryEvaluator.select("SELECT * FROM shards WHERE hostname = ?", hostname) { row => rowToShardInfo(row) }.toList
  }

  def getBusyShards() = {
    queryEvaluator.select("SELECT * FROM shards where busy != 0") { row => rowToShardInfo(row) }.toList
  }

  def getChildShardsOfClass(parentId: ShardId, className: String) = {
    val links = listDownwardLinks(parentId)
    links.map { link => getShard(link.downId) }.filter { _.className == className }.toList ++
      links.flatMap { link => getChildShardsOfClass(link.downId, className) }
  }

  def reload() {
    try {
      List("shards", "shard_children", "forwardings", "sequence").foreach { table =>
        queryEvaluator.select("DESCRIBE " + table) { row => }
      }
    } catch {
      case e: SQLException =>
        // try creating the schema
        rebuildSchema()
    }
  }

  def rebuildSchema() {
    queryEvaluator.execute(SqlShard.SHARDS_DDL)
    queryEvaluator.execute(SqlShard.SHARD_CHILDREN_DDL)
    queryEvaluator.execute(SqlShard.FORWARDINGS_DDL)
  }
}
