package com.twitter.gizzard.nameserver

import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import scala.collection.mutable
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.gizzard.util.TreeUtils
import com.twitter.gizzard.shards._


object SqlShard {
  val SHARDS_DDL = """
CREATE TABLE IF NOT EXISTS shards (
    class_name              VARCHAR(125) NOT NULL,
    table_prefix            VARCHAR(125) NOT NULL,
    hostname                VARCHAR(125)  NOT NULL,
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
    updated_seq             BIGINT                  NOT NULL,

    PRIMARY KEY         (base_source_id, table_id),
    UNIQUE unique_shard (shard_hostname, shard_table_prefix),
    INDEX  updated_seq  (updated_seq)
) ENGINE=INNODB;
"""

  val UPDATE_COUNTER_DDL = """
CREATE TABLE IF NOT EXISTS update_counters (
    id                      VARCHAR(25) NOT NULL,
    counter                 BIGINT      NOT NULL DEFAULT 0,

    PRIMARY KEY (id)
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


class SqlShardManagerSource(queryEvaluator: QueryEvaluator) extends ShardManagerSource {

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


  // Forwardings/Shard Management Write Methods

  def createShard(shardInfo: ShardInfo) {
    try {
      queryEvaluator.transaction { transaction =>
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
        }
      }

      markAncestorForwardingsAsUpdated(shardInfo.id)
    } catch {
      case e: SQLIntegrityConstraintViolationException =>
        throw new InvalidShard("SQL Error: %s".format(e.getMessage))
    }
  }

  def deleteShard(id: ShardId) {
    if (listUpwardLinks(id).length > 0) {
      throw new ShardException("Shard still has links")
    }
    if (listDownwardLinks(id).length > 0) {
      throw new ShardException("Shard still has links")
    }
    queryEvaluator.execute("DELETE FROM shards WHERE hostname = ? AND table_prefix = ?", id.hostname, id.tablePrefix) == 0

    markAncestorForwardingsAsUpdated(id)
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

    Seq(upId, downId).foreach(markAncestorForwardingsAsUpdated)
  }

  def removeLink(upId: ShardId, downId: ShardId) {
    queryEvaluator.execute("DELETE FROM shard_children WHERE parent_hostname = ? AND parent_table_prefix = ? AND child_hostname = ? AND child_table_prefix = ?",
      upId.hostname, upId.tablePrefix, downId.hostname, downId.tablePrefix) == 0

    Seq(upId, downId).foreach(markAncestorForwardingsAsUpdated)
  }

  private def insertOrUpdateForwarding(evaluator: QueryEvaluator, f: Forwarding, deleted: Boolean) {
    val deletedInt = if (deleted) 1 else 0

    evaluator.transaction { t =>
      t.execute("INSERT INTO update_counters (id, counter) VALUES ('forwardings', 1) ON DUPLICATE KEY UPDATE counter = counter + 1")

      val updateCounter = t.selectOne("SELECT counter FROM update_counters WHERE id = 'forwardings' FOR UPDATE")(_.getLong("counter")).get
      val query = "INSERT INTO forwardings (base_source_id, table_id, shard_hostname, shard_table_prefix, deleted, updated_seq) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE " +
                  "shard_hostname = VALUES(shard_hostname), shard_table_prefix = VALUES(shard_table_prefix), " +
                  "deleted = VALUES(deleted), updated_seq = VALUES(updated_seq)"

      t.execute(query, f.baseId, f.tableId, f.shardId.hostname, f.shardId.tablePrefix, deletedInt, updateCounter)
    }
  }

  def setForwarding(f: Forwarding) { insertOrUpdateForwarding(queryEvaluator, f, false) }

  def removeForwarding(f: Forwarding) { insertOrUpdateForwarding(queryEvaluator, f, true) }

  def replaceForwarding(oldId: ShardId, newId: ShardId) {
    val query       = "SELECT * FROM forwardings WHERE shard_hostname = ? AND shard_table_prefix = ?"

    queryEvaluator.transaction { t =>
      val forwardings = t.select(query, oldId.hostname, oldId.tablePrefix)(rowToForwarding)

      forwardings.foreach { case Forwarding(tableId, baseId, _) =>
        insertOrUpdateForwarding(t, Forwarding(tableId, baseId, newId), false)
      }
    }
  }

  def markAncestorForwardingsAsUpdated(id: ShardId) {
    import TreeUtils._

    val ancestorIds = collectFromTree(List(id))(listUpwardLinks(_).map(_.upId))(identity).toSet + id

    ancestorIds.foreach(id => replaceForwarding(id, id))
  }


  // Forwardings/Shard Management Read Methods

  private def loadState() = dumpStructure(listTables)

  private def updateState(state: Seq[NameServerState], updatedSequence: Long) = {
    import TreeUtils._

    val oldForwardings = state.flatMap(_.forwardings).map(f => (f.tableId, f.baseId) -> f).toMap
    val oldLinks       = state.flatMap(_.links).toSet
    val oldLinksByUpId = mapOfSets(oldLinks)(_.upId)
    val oldShards      = state.flatMap(_.shards).map(s => s.id -> s).toMap
    val oldShardIds    = oldShards.keySet

    val newForwardings     = mutable.Map[(Int,Long), Forwarding]()
    val deletedForwardings = mutable.Map[(Int,Long), Forwarding]()

    queryEvaluator.select("SELECT * FROM forwardings WHERE updated_seq > ?", updatedSequence) { row =>
      val f  = rowToForwarding(row)
      val fs = if (row.getBoolean("deleted")) deletedForwardings else newForwardings

      fs += (f.tableId, f.baseId) -> f
    }

    val newRootIds  = newForwardings.map(_._2.shardId).toSet
    val newLinks    = descendantLinks(newRootIds)(listDownwardLinks)
    val newShardIds = newRootIds ++ newLinks.map(_.downId)
    val newShards   = newShardIds.toList.map(id => id -> getShard(id)).toMap

    val purgeableRootIds  = newRootIds ++ deletedForwardings.map(_._2.shardId)
    val purgeableLinks    = descendantLinks(purgeableRootIds)(oldLinksByUpId)
    val purgeableShardIds = purgeableRootIds ++ purgeableLinks.map(_.downId)

    val updatedForwardings = (oldForwardings -- deletedForwardings.keys) ++ newForwardings
    val updatedLinks       = (oldLinks -- purgeableLinks) ++ newLinks
    val updatedShards      = (oldShards -- purgeableShardIds) ++ newShards

    val forwardingsByTableId = mapOfSets(updatedForwardings.map(_._2))(_.tableId)
    val linksByUpId          = mapOfSets(updatedLinks)(_.upId)
    val tableIds             = forwardingsByTableId.keySet

    def extractor(id: Int) = NameServerState.extractTable(id)(forwardingsByTableId)(linksByUpId)(updatedShards)

    tableIds.map(t => extractor(t)).toSeq
  }

  @volatile private var _forwardingUpdatedSeq = 0L
  @volatile private var _currentState: Seq[NameServerState] = null

  private def latestUpdatedSeq() = {
    val query = "SELECT counter FROM update_counters WHERE id = 'forwardings'"
    queryEvaluator.selectOne(query)(_.getLong("counter")).getOrElse(0L)
  }

  def currentState() = {
    synchronized {
      val nextUpdatedSeq = latestUpdatedSeq()

      if (_currentState eq null) {
        _currentState = loadState()
      } else {
        _currentState = updateState(_currentState, _forwardingUpdatedSeq)
      }

      _forwardingUpdatedSeq = nextUpdatedSeq

      _currentState
    }
  }

  def getShard(id: ShardId) = {
    val query = "SELECT * FROM shards WHERE hostname = ? AND table_prefix = ?"
    queryEvaluator.selectOne(query, id.hostname, id.tablePrefix)(rowToShardInfo) getOrElse {
      throw new NonExistentShard("Shard not found: %s".format(id))
    }
  }

  def listTables() = {
    queryEvaluator.select("SELECT DISTINCT table_id FROM forwardings")(_.getInt("table_id")).toList.sortWith((a,b) => a < b)
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
      synchronized {
        _currentState         = null
        _forwardingUpdatedSeq = 0L
      }

      List("shards", "shard_children", "forwardings", "update_counters", "hosts").foreach { table =>
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
    queryEvaluator.execute(SqlShard.UPDATE_COUNTER_DDL)
  }
}

class SqlRemoteClusterManagerSource(queryEvaluator: QueryEvaluator) extends RemoteClusterManagerSource {

  private def rowToHost(row: ResultSet) = {
    Host(row.getString("hostname"),
         row.getInt("port"),
         row.getString("cluster"),
         HostStatus(row.getInt("status")))
  }

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

  def listRemoteClusters() = {
    queryEvaluator.select("SELECT DISTINCT cluster FROM hosts")(_.getString("cluster")).toList
  }

  def listRemoteHosts() = {
    queryEvaluator.select("SELECT * FROM hosts")(rowToHost).toList
  }

  def listRemoteHostsInCluster(c: String) = {
    queryEvaluator.select("SELECT * FROM hosts WHERE cluster = ?", c)(rowToHost).toList
  }

  def reload() {
    try {
      List("hosts").foreach { table =>
        queryEvaluator.select("DESCRIBE " + table) { row => }
      }
    } catch {
      case e: SQLException =>
        // try creating the schema
        rebuildSchema()
    }
  }

  def rebuildSchema() {
    queryEvaluator.execute(SqlShard.HOSTS_DDL)
  }

}
