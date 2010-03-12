package com.twitter.gizzard.nameserver

import java.sql.{ResultSet, SQLException, SQLIntegrityConstraintViolationException}
import java.util.TreeMap
import scala.collection.mutable
import com.twitter.querulous.evaluator.QueryEvaluator
import net.lag.logging.Logger
import jobs.JobScheduler
import shards._


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
        Busy(result.getInt("busy")), result.getInt("id"))
      newShardInfos += (result.getInt("id") -> shardInfo)
    }
    shardInfos = newShardInfos

    val newFamilyTree = new mutable.HashMap[Int, mutable.ArrayBuffer[ChildInfo]] { override def initialSize = shardInfos.size * 10 }
    queryEvaluator.select("SELECT * FROM shard_children ORDER BY parent_id, weight DESC") { result =>
      val children = newFamilyTree.getOrElseUpdate(result.getInt("parent_id"), new mutable.ArrayBuffer[ChildInfo])
      children += ChildInfo(result.getInt("child_id"), result.getInt("weight"))
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

class NameServer[S <: Shard](queryEvaluator: QueryEvaluator, shardRepository: ShardRepository[S],
                             tablePrefix: String, mappingFunction: Long => Long,  copyManager: CopyManager[S]) {
    val FORWARDINGS_DDL = """
  CREATE TABLE """ + tablePrefix + """_forwardings (
      base_source_id          BIGINT                  NOT NULL,
      table_id                VARCHAR(255)            NOT NULL,
      shard_id                INT                     NOT NULL,

      PRIMARY KEY (base_source_id, table_id),

      UNIQUE unique_shard_id (shard_id)
  ) ENGINE=INNODB;
  """

    val SEQUENCE_DDL = """
  CREATE TABLE IF NOT EXISTS """ + tablePrefix + """_sequence (
      id                      INT UNSIGNED            NOT NULL
  ) ENGINE=INNODB;
  """

  private def rowToShardInfo(row: ResultSet) = {
    new ShardInfo(row.getString("class_name"), row.getString("table_prefix"), row.getString("hostname"),
      row.getString("source_type"), row.getString("destination_type"), Busy(row.getInt("busy")),
      row.getInt("id"))
  }

  private def rowToForwarding(row: ResultSet) = {
    new Forwarding(List.fromString(row.getString("table_id"), '.').map(_.toInt), row.getLong("base_source_id"), row.getInt("shard_id"))
  }

  @volatile private var forwardings: scala.collection.Map[(Int, Int), TreeMap[Long, S]] = null
  private val forwardingTable = tablePrefix + "_forwardings"
  private def tableIdDbString(tableId: List[Int]) = tableId.mkString(".")
  private def tableIdTuple(tableId: List[Int]) = (tableId(0), tableId(1))

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

  def addChildShard(parentShardId: Int, childShardId: Int, weight: Int) {
    queryEvaluator.execute("INSERT INTO shard_children (parent_id, child_id, weight) VALUES (?, ?, ?)",
      parentShardId, childShardId, weight)
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
    queryEvaluator.select("SELECT child_id, weight FROM shard_children WHERE parent_id = ? ORDER BY weight DESC", shardId) { row =>
      new ChildInfo(row.getInt("child_id"), row.getInt("weight"))
    }.toList
  }

  def markShardBusy(shardId: Int, busy: Busy.Value) {
    if (queryEvaluator.execute("UPDATE shards SET busy = ? WHERE id = ?", busy.id, shardId) == 0) {
      throw new NameServer.NonExistentShard
    }
  }

  def copyShard(sourceShardId: Int, destinationShardId: Int) {
    copyManager.newCopyJob(sourceShardId, destinationShardId).start(this, copyManager.scheduler)
  }

  def setupMigration(sourceShardInfo: ShardInfo, destinationShardInfo: ShardInfo) = {
    val lastDot = sourceShardInfo.className.lastIndexOf('.')
    val packageName = if (lastDot >= 0) sourceShardInfo.className.substring(0, lastDot + 1) else ""
    val sourceShardId = findShard(sourceShardInfo)
    val destinationShardId = createShard(destinationShardInfo)

    val writeOnlyShard = new ShardInfo(packageName + "WriteOnlyShard",
      sourceShardInfo.tablePrefix + "_migrate_write_only", "localhost", "", "", Busy.Normal, 0)
    val writeOnlyShardId = createShard(writeOnlyShard)
    addChildShard(writeOnlyShardId, destinationShardId, 1)

    val replicatingShard = new ShardInfo(packageName + "ReplicatingShard",
      sourceShardInfo.tablePrefix + "_migrate_replicating", "localhost", "", "", Busy.Normal, 0)
    val replicatingShardId = createShard(replicatingShard)
    replaceChildShard(sourceShardId, replicatingShardId)
    addChildShard(replicatingShardId, sourceShardId, 1)
    addChildShard(replicatingShardId, writeOnlyShardId, 1)

    replaceForwarding(sourceShardId, replicatingShardId)
    new ShardMigration(sourceShardId, destinationShardId, replicatingShardId, writeOnlyShardId)
  }

  def migrateShard(migration: ShardMigration) {
    copyManager.newMigrateJob(migration).start(this, copyManager.scheduler)
  }

  // to be called by Migrate jobs when they're finished. but also available as an RPC.
  def finishMigration(migration: ShardMigration) {
    removeChildShard(migration.writeOnlyShardId, migration.destinationShardId)
    replaceChildShard(migration.replicatingShardId, migration.destinationShardId)
    replaceForwarding(migration.replicatingShardId, migration.destinationShardId)
    deleteShard(migration.replicatingShardId)
    deleteShard(migration.writeOnlyShardId)
    deleteShard(migration.sourceShardId)
  }

  def setForwarding(forwarding: Forwarding) {
    if (queryEvaluator.execute("UPDATE " + forwardingTable + " SET shard_id = ? WHERE base_source_id = ? AND table_id = ?",
                               forwarding.shardId, forwarding.baseId, tableIdDbString(forwarding.tableId)) == 0) {
      queryEvaluator.execute("INSERT INTO " + forwardingTable + " (base_source_id, table_id, shard_id) VALUES (?, ?, ?)",
                             forwarding.baseId, tableIdDbString(forwarding.tableId), forwarding.shardId)
    }
  }

  def replaceForwarding(oldShardId: Int, newShardId: Int) {
    queryEvaluator.execute("UPDATE " + forwardingTable + " SET shard_id = ? WHERE shard_id = ?", newShardId, oldShardId)
  }

  def getForwarding(tableId: List[Int], baseId: Long): ShardInfo = {
    getShard(queryEvaluator.select("SELECT shard_id FROM " + forwardingTable + " WHERE base_source_id = ? AND table_id = ?",
                          baseId, tableIdDbString(tableId)) { row =>
      row.getInt("shard_id")
    }.firstOption.getOrElse { throw new ShardException("No such forwarding") })
  }

  def getForwardingForShard(shardId: Int): Forwarding = {
    queryEvaluator.select("SELECT * FROM " + forwardingTable + " WHERE shard_id = ?", shardId) { row =>
      rowToForwarding(row)
    }.firstOption.getOrElse { throw new ShardException("No such forwarding") }
  }

  def getForwardings(): List[Forwarding] = {
    // XXX: REVISIT ORDERING
    queryEvaluator.select("SELECT * FROM " + forwardingTable + " ORDER BY table_id, base_source_id DESC") { row =>
      rowToForwarding(row)
    }.toList
  }

  def findCurrentForwarding(tableId: List[Int], id: Long): S = {
    forwardings.get(tableIdTuple(tableId)).flatMap { bySourceIds =>
      val item = bySourceIds.floorEntry(mappingFunction(id))
      if (item != null) {
        Some(item.getValue)
      } else {
        None
      }
    } getOrElse {
      throw new NameServer.NonExistentShard
    }
  }

  def reloadForwardings() {
    val newForwardings = new mutable.HashMap[(Int, Int), TreeMap[Long, S]]
    queryEvaluator.select("SELECT * FROM " + forwardingTable) { result =>
      val treeMap = newForwardings.getOrElseUpdate(tableIdTuple(List.fromString(result.getString("table_id"), '.').map(_.toInt)), new TreeMap[Long, S])
      treeMap.put(result.getLong("base_source_id"), findShardById(result.getInt("shard_id")))
    }
    forwardings = newForwardings.readOnly
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
    try {
      List("shards", "shard_children", forwardingTable, tablePrefix + "_sequence").foreach { table =>
        queryEvaluator.select("DESCRIBE " + table) { row => }
      }
    } catch {
      case e: SQLException =>
        // try creating the schema
        rebuildSchema()
    }

    NameServer.reload(queryEvaluator)
    reloadForwardings()
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

  def rebuildSchema() {
    NameServer.rebuildSchema(queryEvaluator)
    queryEvaluator.execute("DROP TABLE IF EXISTS " + forwardingTable)
    queryEvaluator.execute("DROP TABLE IF EXISTS " + tablePrefix + "_sequence")
    queryEvaluator.execute(FORWARDINGS_DDL)
    queryEvaluator.execute(SEQUENCE_DDL)
    queryEvaluator.execute("INSERT INTO " + tablePrefix + "_sequence VALUES (0)")
  }
}
