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
    deleted                TINYINT      NOT NULL DEFAULT 0,

   PRIMARY KEY primary_key_table_prefix_hostname (hostname, table_prefix),
   INDEX deleted (deleted),
   INDEX busy (busy)
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


class SqlShard(queryEvaluator: QueryEvaluator) extends nameserver.Shard {
  val children = List()
  val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.SqlShard", "", "")
  val weight = 1 // hardcode for now

  private def rowToShardInfo(row: ResultSet) = {
    ShardInfo(
      ShardId(row.getString("hostname"),
      row.getString("table_prefix")),
      row.getString("class_name"),
      row.getString("source_type"),
      row.getString("destination_type"),
      Busy(row.getInt("busy")),
      Deleted(row.getInt("deleted")))
  }

  private def rowToForwarding(row: ResultSet) = {
    Forwarding(
      row.getInt("table_id"),
      row.getLong("base_source_id"),
      ShardId(row.getString("shard_hostname"), row.getString("shard_table_prefix")))
  }

  private def rowToLinkInfo(row: ResultSet) = {
    LinkInfo(
      ShardId(row.getString("parent_hostname"), row.getString("parent_table_prefix")),
      ShardId(row.getString("child_hostname"), row.getString("child_table_prefix")),
      row.getInt("weight"))
  }

  private def lookupShard(evaluator: QueryEvaluator, deleted: Seq[Deleted.Value], id: ShardId): Option[ShardInfo] = {
    val queryParameters = deleted.map(_.id) :: List(id.hostname, id.tablePrefix)
    evaluator.selectOne("SELECT * FROM shards WHERE deleted IN (?) AND hostname = ? AND table_prefix = ?",
      queryParameters: _*)(rowToShardInfo)
  }
  private def lookupShard(deleted: Deleted.Value, id: ShardId): Option[ShardInfo] = lookupShard(queryEvaluator, List(deleted), id)

  private def lookupShards(deleted: Seq[Deleted.Value], busy: Seq[Busy.Value]) = {
    val queryParameters = deleted.map(_.id) :: busy.map(_.id) :: Nil
    queryEvaluator.select("SELECT * FROM shards WHERE deleted IN (?) AND busy IN (?)", queryParameters: _*)(rowToShardInfo)
  }

  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]) {
    queryEvaluator.transaction { transaction =>
      try {
        lookupShard(transaction, Deleted.elements.toList, shardInfo.id).map { existing =>
          if ( !existing.isEquivalent(shardInfo) )
            throw new InvalidShard("Invalid shard: %s doesn't match %s".format(existing, shardInfo))

          if ( existing.isDeleted )
            throw new InvalidShard("Invalid shard: %s has been deleted. Undelete or purge and recreate.".format(shardInfo))

        } getOrElse {
          transaction.insert(
            "INSERT INTO shards (hostname, table_prefix, class_name, source_type, destination_type) VALUES (?,?,?,?,?)",
            shardInfo.hostname,
            shardInfo.tablePrefix,
            shardInfo.className,
            shardInfo.sourceType,
            shardInfo.destinationType)

          repository.create(shardInfo)
        }
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          throw new InvalidShard("SQL Error: %s".format(e.getMessage))
      }
    }
  }

  def getShard(id: ShardId) = {
    lookupShard(Deleted.Normal, id).getOrElse {
      throw new NonExistentShard("Shard not found: %s".format(id))
    }
  }

  def listHostnames() = {
    queryEvaluator.select("SELECT DISTINCT hostname FROM shards WHERE deleted = ?", Deleted.Normal.id) { row =>
      row.getString("hostname")
    }
  }

  def removeForwarding(f: Forwarding) {
    queryEvaluator.execute("DELETE FROM forwardings WHERE base_source_id = ? AND " +
                           "shard_hostname = ? AND shard_table_prefix = ? AND " +
                           "table_id = ? LIMIT 1",
                           f.baseId, f.shardId.hostname, f.shardId.tablePrefix, f.tableId)
  }

  def deleteShard[S <: shards.Shard](id: ShardId, repository: ShardRepository[S]) {
    queryEvaluator.execute("DELETE FROM shard_children WHERE "+
                           "(parent_hostname = ? AND parent_table_prefix = ?) OR " +
                           "(child_hostname = ? AND child_table_prefix = ?)",
                           id.hostname, id.tablePrefix, id.hostname, id.tablePrefix)

    queryEvaluator.execute("UPDATE shards SET deleted = ? WHERE hostname = ? AND table_prefix = ?",
                           Deleted.Deleted.id, id.hostname, id.tablePrefix)

    lookupShard(Deleted.Deleted, id).map { info =>
      repository.factory(info.className) match {
        case s: shards.AbstractShardFactory[_] => purgeShard(info, repository)
        case _ =>
      }
    }
  }

  def purgeShard[S <: shards.Shard](info: ShardInfo, repository: ShardRepository[S]) {
    if ( queryEvaluator.execute("DELETE FROM shards WHERE deleted = ? AND hostname = ? AND table_prefix = ?",
      Deleted.Deleted.id, info.hostname, info.tablePrefix) > 0 ) {

      repository.purge(info)
    }
  }

  def purgeShard[S <: shards.Shard](id: ShardId, repository: ShardRepository[S]) {
    val info = lookupShard(Deleted.Deleted, id).getOrElse( throw new NonExistentShard("Shard not found: %s".format(id)) )

    purgeShard(info, repository)
  }

  def markShardBusy(id: ShardId, busy: Busy.Value) {
    if (queryEvaluator.execute("UPDATE shards SET busy = ? WHERE deleted = ? AND hostname = ? AND table_prefix = ?",
      busy.id, Deleted.Normal.id, id.hostname, id.tablePrefix) == 0) {
      throw new NonExistentShard("Could not find shard: %s".format(id))
    }
  }

  def listShards() = lookupShards(List(Deleted.Normal), Busy.elements.toList).toList

  def shardsForHostname(hostname: String) = listShards().filter( shard => shard.hostname == hostname )

  def getBusyShards() = lookupShards(List(Deleted.Normal), List(Busy.Busy)).toList

  def getDeletedShards() = lookupShards(List(Deleted.Deleted), Busy.elements.toList).toList

  def addLink(upId: ShardId, downId: ShardId, weight: Int) {
    if (upId == downId) {
      throw new ShardException("Can't link shard to itself")
    }
    queryEvaluator.execute("REPLACE shard_children (parent_hostname, parent_table_prefix, child_hostname, child_table_prefix, weight) VALUES (?, ?, ?, ?, ?)",
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

  def getChildShardsOfClass(parentId: ShardId, className: String) = {
    val links = listDownwardLinks(parentId)
    links.map { link => getShard(link.downId) }.filter { _.className == className }.toList ++
      links.flatMap { link => getChildShardsOfClass(link.downId, className) }
  }

  def reload() {
    try {
      List("shards", "shard_children", "forwardings").foreach { table =>
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
