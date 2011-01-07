// package com.twitter.gizzard.integration
// 
// import com.twitter.gizzard.thrift.conversions.Sequences._
// import testserver._
// import testserver.config.TestServerConfig
// import testserver.thrift.TestResult
// import java.io.File
// import scheduler.cursor._
// import java.util.concurrent.atomic.AtomicInteger
// import org.specs.mock.{ClassMocker, JMocker}
// import net.lag.configgy.{Config => CConfig}
// import com.twitter.util.TimeConversions._
// import com.twitter.gizzard.thrift.{JobInjectorService, TThreadServer, JobInjector}
// import nameserver.{Host, HostStatus, JobRelay}
// 
// class MulticopyIntegrationSpec extends IntegrationSpecification with ConfiguredSpecification {
//   override def testServer(i: Int) = {
//     val port = 8000 + (i - 1) * 3
//     val name = "testserver" + i
//     new TestServer(TestServerConfig(name, port)) with TestServerFacts {
//       val enum = i
//       val nsDatabaseName = "gizzard_test_"+name+"_ns"
//       val databaseName   = "gizzard_test_"+name
//       val basePort       = port
//       val injectorPort   = port + 1
//       val managerPort    = port + 2
//       val sqlShardInfos = List(
//         shards.ShardInfo(shards.ShardId("localhost", "t0_0"),
//                                         "TestShard", "int", "int", shards.Busy.Normal),
//         shards.ShardInfo(shards.ShardId("localhost", "t0_1"),
//                                         "TestShard", "int", "int", shards.Busy.Normal),
//         shards.ShardInfo(shards.ShardId("localhost", "t0_2"),
//                                         "TestShard", "int", "int", shards.Busy.Normal)
//       )
//       val forwardings = List(
//         nameserver.Forwarding(0, 0, sqlShardInfos.first.id)
//       )
//       val kestrelQueues = Seq("gizzard_test_"+name+"_high_queue",
//                               "gizzard_test_"+name+"_high_queue_errors",
//                               "gizzard_test_"+name+"_low_queue",
//                               "gizzard_test_"+name+"_low_queue_errors")
//     }
//   }
//   
//   "Multicopy" should {
//     val server = testServer(1)
//     val client = testServerClient(server)
// 
//     doBefore {
//       resetTestServerDBs(server)
//       setupServers(server)
//       server.nameServer.reload()
//     }
// 
//     doAfter { stopServers(server) }
//     
//     "copy to multiple locations" in {
//       startServers(server)
//       val nameserver = server.nameServer
//     
//       val sourceInfo :: dest0info :: dest2info :: _ = server.sqlShardInfos
//     
//       client.put(0, "foo")
//       client.put(1, "bar")
//       client.put(2, "baz")
//       client.put(3, "bonk")
//       client.get(3) must eventually(be_==(List(new TestResult(3, "bonk", 1)).toJavaList))
//     
//       val dest0 = CopyDestination(dest0info.id, Some(0))
//       val dest2 = CopyDestination(dest2info.id, Some(2))
//     
//       val copy = new TestSplitFactory(server.nameServer, server.jobScheduler(Priority.Low.id))(sourceInfo.id, List(dest0, dest2))
//     
//       copy()
//           
//       val sourceShard = nameserver.findShardById(sourceInfo.id)
//       val dest0Shard = nameserver.findShardById(dest0info.id)
//       val dest2Shard = nameserver.findShardById(dest2info.id)
//       
//       dest0Shard.get(0) must eventually(be_==(Some((0, "foo", 1))))
//       dest0Shard.get(1) must eventually(be_==(Some((1, "bar", 1))))
//       dest0Shard.get(2) must eventually(be_==(None))
//       dest0Shard.get(3) must eventually(be_==(None))
//       
//       dest2Shard.get(0) must eventually(be_==(None))
//       dest2Shard.get(1) must eventually(be_==(None))
//       dest2Shard.get(2) must eventually(be_==(Some((2, "baz", 1))))
//       dest2Shard.get(3) must eventually(be_==(Some((3, "bonk", 1))))
//     }
//     
//     
//   
//   }
// }
// 
