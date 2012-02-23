package com.twitter.gizzard.nameserver

import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import com.twitter.logging.Logger
import com.twitter.querulous.evaluator.{QueryEvaluator, Transaction}
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

  def rowToShardInfo(row: ResultSet) = {
    ShardInfo(ShardId(row.getString("hostname"), row.getString("table_prefix")), row.getString("class_name"),
      row.getString("source_type"), row.getString("destination_type"), Busy(row.getInt("busy")))
  }

  def rowToForwarding(row: ResultSet) = {
    Forwarding(row.getInt("table_id"), row.getLong("base_source_id"), ShardId(row.getString("shard_hostname"), row.getString("shard_table_prefix")))
  }

  def rowToLinkInfo(row: ResultSet) = {
    LinkInfo(ShardId(row.getString("parent_hostname"), row.getString("parent_table_prefix")),
             ShardId(row.getString("child_hostname"), row.getString("child_table_prefix")),
             row.getInt("weight"))
  }
}


class SqlShardManagerTransaction(transaction : Transaction) extends ShardManagerSource {
  // Forwardings/Shard Management Write Methods

  def createShard(shardInfo: ShardInfo) {
    try {
      transaction.selectOne("SELECT * FROM shards WHERE table_prefix = ? AND hostname = ?",
                            shardInfo.tablePrefix, shardInfo.hostname) { row =>
        if (row.getString("class_name") != shardInfo.className ||
            row.getString("source_type") != shardInfo.sourceType ||
            row.getString("destination_type") != shardInfo.destinationType) {
          throw new InvalidShard("Invalid shard: %s doesn't match %s".format(SqlShard.rowToShardInfo(row), shardInfo))
        }
      } getOrElse {
        transaction.insert("INSERT INTO shards (hostname, table_prefix, class_name, " +
                           "source_type, destination_type) VALUES (?, ?, ?, ?, ?)",
                           shardInfo.hostname, shardInfo.tablePrefix, shardInfo.className,
                           shardInfo.sourceType, shardInfo.destinationType)
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
    transaction.execute("DELETE FROM shards WHERE hostname = ? AND table_prefix = ?", id.hostname, id.tablePrefix) == 0

    markAncestorForwardingsAsUpdated(id)
  }

  def addLink(upId: ShardId, downId: ShardId, weight: Int) {
    if (upId == downId) {
      throw new ShardException("Can't link shard to itself")
    }
    // Links to non-existant shards are a bad thing
    getShard(upId)
    getShard(downId)
    transaction.execute("INSERT INTO shard_children (parent_hostname, parent_table_prefix, child_hostname, child_table_prefix, weight) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE weight=VALUES(weight)",
      upId.hostname, upId.tablePrefix, downId.hostname, downId.tablePrefix, weight)
    // XXX: todo - check loops

    Seq(upId, downId).foreach(markAncestorForwardingsAsUpdated)
  }

  def removeLink(upId: ShardId, downId: ShardId) {
    transaction.execute("DELETE FROM shard_children WHERE parent_hostname = ? AND parent_table_prefix = ? AND child_hostname = ? AND child_table_prefix = ?",
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

  def setForwarding(f: Forwarding) { insertOrUpdateForwarding(transaction, f, false) }

  def removeForwarding(f: Forwarding) { insertOrUpdateForwarding(transaction, f, true) }

  def replaceForwarding(oldId: ShardId, newId: ShardId) {
    val query       = "SELECT * FROM forwardings WHERE shard_hostname = ? AND shard_table_prefix = ?"

    val forwardings = transaction.select(query, oldId.hostname, oldId.tablePrefix)(SqlShard.rowToForwarding)

    forwardings.foreach { case Forwarding(tableId, baseId, _) =>
      insertOrUpdateForwarding(transaction, Forwarding(tableId, baseId, newId), false)
    }
  }

  def markAncestorForwardingsAsUpdated(id: ShardId) {
    import TreeUtils._

    val ancestorIds = collectFromTree(List(id))(listUpwardLinks(_).map(_.upId))(identity).toSet + id

    ancestorIds.foreach(id => replaceForwarding(id, id))
  }

  def getMasterStateVersion() : Long = {
    val query = "SELECT counter FROM update_counters WHERE id = 'version'"
    transaction.selectOne(query)(_.getLong("counter")).getOrElse(0L)
  }

  def getCurrentStateVersion() : Long = {
    throw new UnsupportedOperationException("Transactional contexts do not have an independent state version")
  }

  def incrementStateVersion() {
    transaction.execute(
      "INSERT INTO update_counters (id, counter) VALUES ('version', 1) ON DUPLICATE KEY UPDATE counter = counter + 1")
  }

  def getShard(id: ShardId) = {
    val query = "SELECT * FROM shards WHERE hostname = ? AND table_prefix = ?"
    transaction.selectOne(query, id.hostname, id.tablePrefix)(SqlShard.rowToShardInfo) getOrElse {
      throw new NonExistentShard("Shard not found: %s".format(id))
    }
  }

  def listTables() = {
    transaction.select("SELECT DISTINCT table_id FROM forwardings")(_.getInt("table_id")).toList.sortWith((a,b) => a < b)
  }

  def listHostnames() = {
    transaction.select("SELECT DISTINCT hostname FROM shards")(_.getString("hostname")).toList
  }

  def listLinks() = {
    transaction.select("SELECT * FROM shard_children ORDER BY parent_hostname, parent_table_prefix")(SqlShard.rowToLinkInfo).toList
  }

  def listDownwardLinks(id: ShardId) = {
    val query = "SELECT * FROM shard_children WHERE parent_hostname = ? AND parent_table_prefix = ? ORDER BY weight DESC"
    transaction.select(query, id.hostname, id.tablePrefix)(SqlShard.rowToLinkInfo).toList
  }

  def listUpwardLinks(id: ShardId) = {
    val query = "SELECT * FROM shard_children WHERE child_hostname = ? AND child_table_prefix = ? ORDER BY weight DESC"
    transaction.select(query, id.hostname, id.tablePrefix)(SqlShard.rowToLinkInfo).toList
  }

  def listShards() = {
    transaction.select("SELECT * FROM shards")(SqlShard.rowToShardInfo).toList
  }

  def markShardBusy(id: ShardId, busy: Busy.Value) {
    val query = "UPDATE shards SET busy = ? WHERE hostname = ? AND table_prefix = ?"
    if (transaction.execute(query, busy.id, id.hostname, id.tablePrefix) == 0) {
      throw new NonExistentShard("Could not find shard: %s".format(id))
    }
  }

  def getForwarding(tableId: Int, baseId: Long) = {
    val query = "SELECT * FROM forwardings WHERE base_source_id = ? AND table_id = ? AND deleted = 0"
    transaction.selectOne(query, baseId, tableId)(SqlShard.rowToForwarding) getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardingForShard(id: ShardId) = {
    val query = "SELECT * FROM forwardings WHERE shard_hostname = ? AND shard_table_prefix = ? AND deleted = 0"
    transaction.selectOne(query, id.hostname, id.tablePrefix)(SqlShard.rowToForwarding) getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardings() = {
    transaction.select("SELECT * FROM forwardings WHERE deleted = 0 ORDER BY table_id, base_source_id ASC")(SqlShard.rowToForwarding).toList
  }

  def getForwardingsForTableIds(tableIds: Seq[Int]) = {
    transaction.select("SELECT * FROM forwardings WHERE table_id IN (?) AND deleted = 0 ORDER BY table_id, base_source_id ASC", tableIds)(SqlShard.rowToForwarding).toList
  }

  def shardsForHostname(hostname: String) = {
    transaction.select("SELECT * FROM shards WHERE hostname = ?", hostname)(SqlShard.rowToShardInfo).toList
  }

  def getBusyShards() = {
    transaction.select("SELECT * FROM shards where busy != 0")(SqlShard.rowToShardInfo).toList
  }

  def latestUpdatedSeq() = {
    val query = "SELECT counter FROM update_counters WHERE id = 'forwardings'"
    transaction.selectOne(query)(_.getLong("counter")).getOrElse(0L)
  }

  def loadState() = dumpStructure(listTables)

  def batchExecute(commands : Seq[BatchedCommand]) {
    for (cmd <- commands) {
      cmd match {
        case CreateShard(shardInfo) => createShard(shardInfo)
        case DeleteShard(shardId) => deleteShard(shardId)
        case AddLink(upId, downId, weight) => addLink(upId, downId, weight)
        case RemoveLink(upId, downId) => removeLink(upId, downId)
        case SetForwarding(forwarding) => setForwarding(forwarding)
        case RemoveForwarding(forwarding) => removeForwarding(forwarding)
      }
    }
  }

  def updateState(state: Seq[NameServerState], updatedSequence: Long) = {
    import TreeUtils._

    val oldForwardings = state.flatMap(_.forwardings).map(f => (f.tableId, f.baseId) -> f).toMap
    val oldLinks       = state.flatMap(_.links).toSet
    val oldLinksByUpId = mapOfSets(oldLinks)(_.upId)
    val oldShards      = state.flatMap(_.shards).map(s => s.id -> s).toMap
    val oldShardIds    = oldShards.keySet

    val newForwardings     = mutable.Map[(Int,Long), Forwarding]()
    val deletedForwardings = mutable.Map[(Int,Long), Forwarding]()

    transaction.select("SELECT * FROM forwardings WHERE updated_seq > ?", updatedSequence) { row =>
      val f  = SqlShard.rowToForwarding(row)
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

  def reload() {
    throw new UnsupportedOperationException("reload not supported within a transactional context")
  }

  def currentState(): Seq[NameServerState] = {
    throw new UnsupportedOperationException("shard state not supported within a transactional context")
  }
}

class SqlShardManagerSource(queryEvaluator: QueryEvaluator) extends ShardManagerSource {
  private val log = Logger.get(getClass.getName)

  private def withTransaction [T] (f : SqlShardManagerTransaction => T) : T = {
    queryEvaluator.transaction(t => f(new SqlShardManagerTransaction(t)))
  }

  private def updateWithTransaction [T] (f : SqlShardManagerTransaction => T) : T = {
    queryEvaluator.transaction(t => {
      val shardTransaction = new SqlShardManagerTransaction(t)
      val result = f(shardTransaction)
      shardTransaction.incrementStateVersion()
      result
    })
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

  def createShard(shardInfo: ShardInfo) { updateWithTransaction(_.createShard(shardInfo)) }
  def deleteShard(id: ShardId) { updateWithTransaction(_.deleteShard(id)) }
  def markShardBusy(id: ShardId, busy: Busy.Value) { updateWithTransaction(_.markShardBusy(id, busy)) }

  def getShard(id: ShardId) = withTransaction(_.getShard(id))
  def shardsForHostname(hostname: String) = withTransaction(_.shardsForHostname(hostname))
  def listShards() = withTransaction(_.listShards())
  def getBusyShards() = withTransaction(_.getBusyShards())

  def addLink(upId: ShardId, downId: ShardId, weight: Int) {
    updateWithTransaction(_.addLink(upId, downId, weight))
  }

  def removeLink(upId: ShardId, downId: ShardId) {
    updateWithTransaction(_.removeLink(upId, downId))
  }

  def listUpwardLinks(id: ShardId) = withTransaction(_.listUpwardLinks(id))

  def listDownwardLinks(id: ShardId) = withTransaction(_.listDownwardLinks(id))
  def listLinks() = withTransaction(_.listLinks())

  def setForwarding(forwarding: Forwarding) { updateWithTransaction(_.setForwarding(forwarding)) }
  def removeForwarding(forwarding: Forwarding) { updateWithTransaction(_.removeForwarding(forwarding)) }
  def replaceForwarding(oldId: ShardId, newId: ShardId) {
    updateWithTransaction(_.replaceForwarding(oldId, newId))
  }

  def getForwarding(tableId: Int, baseId: Long) = withTransaction(_.getForwarding(tableId, baseId))
  def getForwardingForShard(id: ShardId) = withTransaction(_.getForwardingForShard(id))
  def getForwardings() = withTransaction(_.getForwardings())
  def getForwardingsForTableIds(tableIds: Seq[Int]) = withTransaction(_.getForwardingsForTableIds(tableIds))

  def listHostnames() = withTransaction(_.listHostnames())
  def listTables() = withTransaction(_.listTables())

  def getMasterStateVersion() = withTransaction(_.getMasterStateVersion())
  def incrementStateVersion() { updateWithTransaction(_ => ()) }

  def batchExecute(commands : Seq[BatchedCommand]) { updateWithTransaction(_.batchExecute(commands)) }

  // Forwardings/Shard Management Read Methods

  @volatile private var _forwardingUpdatedSeq = 0L
  @volatile private var _currentState: Seq[NameServerState] = null
  private val _stateVersion = new AtomicLong(0L)

  def currentState() = {
    synchronized {
      withTransaction(t => {
        val nextUpdatedSeq = t.latestUpdatedSeq()

        if (_currentState eq null) {
          _currentState = t.loadState()
        } else {
          _currentState = t.updateState(_currentState, _forwardingUpdatedSeq)
        }

        _forwardingUpdatedSeq = nextUpdatedSeq
        _stateVersion.set(t.getMasterStateVersion())

        _currentState
      })
    }
  }

  def getCurrentStateVersion() : Long = _stateVersion.get()
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
