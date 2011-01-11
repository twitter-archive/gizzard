package com.twitter.gizzard.nameserver

import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import scala.collection.mutable
import com.twitter.querulous.evaluator.QueryEvaluator
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

   PRIMARY KEY (hostname, table_prefix)
) ENGINE=INNODB
"""

  val SHARD_CHILDREN_DDL = """
CREATE TABLE IF NOT EXISTS shard_children (
    parent_hostname         VARCHAR(125) NOT NULL,
    parent_table_prefix     VARCHAR(125) NOT NULL,
    child_hostname          VARCHAR(125) NOT NULL,
    child_table_prefix      VARCHAR(125) NOT NULL,
    weight                  INT          NOT NULL DEFAULT 1,

    PRIMARY KEY (parent_hostname, parent_table_prefix, child_hostname, child_table_prefix),
    INDEX child (child_hostname, child_table_prefix)
) ENGINE=INNODB
"""

  val FORWARDINGS_DDL = """
CREATE TABLE IF NOT EXISTS forwardings (
    base_source_id          BIGINT                  NOT NULL,
    table_id                INT                     NOT NULL,
    shard_hostname          VARCHAR(125)            NOT NULL,
    shard_table_prefix      VARCHAR(125)            NOT NULL,
    deleted                 TINYINT                 NOT NULL DEFAULT 0,
    updated_seq             BIGINT                  NOT NULL AUTO_INCREMENT,

    PRIMARY KEY         (base_source_id, table_id),
    UNIQUE unique_shard (shard_hostname, shard_table_prefix),
    UNIQUE updated_seq  (updated_seq)
) ENGINE=INNODB;
"""

  val HOSTS_DDL = """
CREATE TABLE IF NOT EXISTS hosts (
    hostname                VARCHAR(125) NOT NULL,
    port                    INT          NOT NULL,
    cluster                 VARCHAR(125) NOT NULL,
    status                  INT          NOT NULL DEFAULT 0,

    PRIMARY KEY (hostname, port),
    INDEX cluster (cluster, status)
) ENGINE=INNODB;
"""
}

class SqlShard(queryEvaluator: QueryEvaluator) extends nameserver.Shard {
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

  private def rowToHost(row: ResultSet) = {
    Host(row.getString("hostname"),
         row.getInt("port"),
         row.getString("cluster"),
         HostStatus(row.getInt("status")))
  }


  // Forwardings/Shard Management Write Methods

  def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]) {
    queryEvaluator.transaction { transaction =>
      try {
        transaction.selectOne("SELECT * FROM shards WHERE table_prefix = ? AND hostname = ?",
                              shardInfo.tablePrefix, shardInfo.hostname) { row =>
          if (row.getString("class_name") != shardInfo.className ||
              row.getString("source_type") != shardInfo.sourceType ||
              row.getString("destination_type") != shardInfo.destinationType) {
            throw new InvalidShard("Invalid shard: %s doesn't match %s".format(rowToShardInfo(row), shardInfo))
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
          throw new InvalidShard("SQL Error: %s".format(e.getMessage))
      }
    }
  }

  def deleteShard(id: ShardId) = {
    if (listUpwardLinks(id).length > 0) {
      throw new ShardException("Shard still has links")
    }
    if (listDownwardLinks(id).length > 0) {
      throw new ShardException("Shard still has links")
    }
    queryEvaluator.execute("DELETE FROM shards WHERE hostname = ? AND table_prefix = ?", id.hostname, id.tablePrefix) == 0
  }

  def addLink(upId: ShardId, downId: ShardId, weight: Int) {
    if (upId == downId) {
      throw new ShardException("Can't link shard to itself")
    }
    // Links to non-existant shards are a bad thing
    getShard(upId)
    getShard(downId)
    queryEvaluator.execute("INSERT INTO shard_children (parent_hostname, parent_table_prefix, child_hostname, child_table_prefix, weight) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE weight=VALUES(weight)",
      upId.hostname, upId.tablePrefix, downId.hostname, downId.tablePrefix, weight)
    // XXX: todo - check loops
  }

  def removeLink(upId: ShardId, downId: ShardId) {
    queryEvaluator.execute("DELETE FROM shard_children WHERE parent_hostname = ? AND parent_table_prefix = ? AND child_hostname = ? AND child_table_prefix = ?",
      upId.hostname, upId.tablePrefix, downId.hostname, downId.tablePrefix) == 0
  }

  private def insertOrUpdateForwarding(evaluator: QueryEvaluator, f: Forwarding, deleted: Boolean) {
    val deletedInt = if (deleted) 1 else 0

    val query = "INSERT INTO forwardings (base_source_id, table_id, shard_hostname, shard_table_prefix, deleted) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE " +
                "base_source_id = VALUES(base_source_id), table_id = VALUES(table_id), " +
                "shard_hostname = VALUES(shard_hostname), shard_table_prefix = VALUES(shard_table_prefix), " +
                "deleted = VALUES(deleted), updated_seq = LAST_INSERT_ID(updated_seq) + 1"

    evaluator.execute(query, f.baseId, f.tableId, f.shardId.hostname, f.shardId.tablePrefix, deletedInt)
  }

  def setForwarding(f: Forwarding) { insertOrUpdateForwarding(queryEvaluator, f, false) }

  def removeForwarding(f: Forwarding) { insertOrUpdateForwarding(queryEvaluator, f, true) }

  def replaceForwarding(oldId: ShardId, newId: ShardId) {
    queryEvaluator.transaction { t =>
      val query       = "SELECT * FROM forwardings WHERE shard_hostname = ? AND shard_table_prefix = ? FOR UPDATE"
      val forwardings = t.select(query, oldId.hostname, oldId.tablePrefix)(rowToForwarding)

      forwardings.foreach { case Forwarding(tableId, baseId, _) =>
        insertOrUpdateForwarding(t, Forwarding(tableId, baseId, newId), false)
      }
    }
  }


  // Forwardings/Shard Management Read Methods

  private def loadState() = {
    val tableIds = queryEvaluator.select("SELECT DISTINCT table_id FROM forwardings")(_.getInt("table_id"))
    dumpStructure(tableIds)
  }

  @volatile private var _currentState: Option[Seq[NameServerState]] = None

  def currentState() = {
    syncronized {
      loadState()
    }
  }

  def getShard(id: ShardId) = {
    val query = "SELECT * FROM shards WHERE hostname = ? AND table_prefix = ?"
    queryEvaluator.selectOne(query, id.hostname, id.tablePrefix)(rowToShardInfo) getOrElse {
      throw new NonExistentShard("Shard not found: %s".format(id))
    }
  }

  def listHostnames() = {
    queryEvaluator.select("SELECT DISTINCT hostname FROM shards")(_.getString("hostname")).toList
  }

  def listLinks() = {
    queryEvaluator.select("SELECT * FROM shard_children ORDER BY parent_hostname, parent_table_prefix")(rowToLinkInfo).toList
  }

  def listDownwardLinks(id: ShardId) = {
    val query = "SELECT * FROM shard_children WHERE parent_hostname = ? AND parent_table_prefix = ? ORDER BY weight DESC"
    queryEvaluator.select(query, id.hostname, id.tablePrefix)(rowToLinkInfo).toList
  }

  def listUpwardLinks(id: ShardId) = {
    val query = "SELECT * FROM shard_children WHERE child_hostname = ? AND child_table_prefix = ? ORDER BY weight DESC"
    queryEvaluator.select(query, id.hostname, id.tablePrefix)(rowToLinkInfo).toList
  }

  def listShards() = {
    queryEvaluator.select("SELECT * FROM shards")(rowToShardInfo).toList
  }

  def markShardBusy(id: ShardId, busy: Busy.Value) {
    val query = "UPDATE shards SET busy = ? WHERE hostname = ? AND table_prefix = ?"
    if (queryEvaluator.execute(query, busy.id, id.hostname, id.tablePrefix) == 0) {
      throw new NonExistentShard("Could not find shard: %s".format(id))
    }
  }

  def getForwarding(tableId: Int, baseId: Long) = {
    val query = "SELECT * FROM forwardings WHERE base_source_id = ? AND table_id = ? AND deleted = 0"
    queryEvaluator.selectOne(query, baseId, tableId)(rowToForwarding) getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardingForShard(id: ShardId) = {
    val query = "SELECT * FROM forwardings WHERE shard_hostname = ? AND shard_table_prefix = ? AND deleted = 0"
    queryEvaluator.selectOne(query, id.hostname, id.tablePrefix)(rowToForwarding) getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardings() = {
    queryEvaluator.select("SELECT * FROM forwardings WHERE deleted = 0 ORDER BY table_id, base_source_id ASC")(rowToForwarding).toList
  }

  def getForwardingsForTableIds(tableIds: Seq[Int]) = {
    queryEvaluator.select("SELECT * FROM forwardings WHERE table_id IN (?) AND deleted = 0 ORDER BY table_id, base_source_id ASC", tableIds)(rowToForwarding).toList
  }

  def shardsForHostname(hostname: String) = {
    queryEvaluator.select("SELECT * FROM shards WHERE hostname = ?", hostname)(rowToShardInfo).toList
  }

  def getBusyShards() = {
    queryEvaluator.select("SELECT * FROM shards where busy != 0")(rowToShardInfo).toList
  }

  def reload() {
    try {
      List("shards", "shard_children", "forwardings", "hosts").foreach { table =>
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
    queryEvaluator.execute(SqlShard.HOSTS_DDL)
  }


  // Remote Host Cluster Management

  def addRemoteHost(h: Host) {
    val sql = "INSERT INTO hosts (hostname, port, cluster, status) VALUES (?,?,?,?)" +
              "ON DUPLICATE KEY UPDATE cluster=VALUES(cluster), status=VALUES(status)"
    queryEvaluator.execute(sql, h.hostname, h.port, h.cluster, h.status.id)
  }

  def removeRemoteHost(hostname: String, port: Int) {
    val sql = "DELETE FROM hosts WHERE hostname = ? AND port = ?"
    queryEvaluator.execute(sql, hostname, port)
  }

  def setRemoteHostStatus(hostname: String, port: Int, status: HostStatus.Value) {
    val sql = "UPDATE hosts SET status = ? WHERE hostname = ? AND port = ?"
    if (queryEvaluator.execute(sql, status.id, hostname, port) == 0) {
      throw new ShardException("No such remote host")
    }
  }

  def setRemoteClusterStatus(cluster: String, status: HostStatus.Value) {
    val sql = "UPDATE hosts SET status = ? WHERE cluster = ?"
    queryEvaluator.execute(sql, status.id, cluster)
  }


  def getRemoteHost(host: String, port: Int) = {
    val sql = "SELECT * FROM hosts WHERE hostname = ? AND port = ?"
    queryEvaluator.selectOne(sql, host, port)(rowToHost) getOrElse {
      throw new ShardException("No such remote host")
    }
  }

  def listRemoteClusters() =
    queryEvaluator.select("SELECT DISTINCT cluster FROM hosts")(_.getString("cluster")).toList

  def listRemoteHosts() =
    queryEvaluator.select("SELECT * FROM hosts")(rowToHost).toList

  def listRemoteHostsInCluster(c: String) =
    queryEvaluator.select("SELECT * FROM hosts WHERE cluster = ?", c)(rowToHost).toList
}
