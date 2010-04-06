# Introducing Gizzard, a framework for creating distributed datastores.

## An introduction to sharding

Many modern web sites needs fast access to large amounts of information. One of the common ways to deal with this problem is to "shard" the data. Sharding strategies often involve two techniques: partitioning and replication. With *partitioning*, the data is divided into small chunks and stored across many computers. Each of these chunks is small enough that the computer that stores it can efficiently manipulate and query the data. With *replication*, multiple copies of the data are stored across many machines. Since each copy runs on its own machine can respond to queries, the system can efficiently respond to huge amounts of queries by simply adding more and more copies. Replication also makes the system resilient to failure because if any one copy is broken or corrupt, the system can use another copy for the same task.

But sharding is difficult. Developing with smart partitioning schemes for particular kinds of data requires a lot of thought. And even harder is ensuring that all of the copies of the data are consistent despite unreliable communication and computer failures. Recently, a lot of open-source distributed databases have emerged to help solve this problem. Unfortunately, as of the time of writing, most of the available open-source projects are either too buggy or too limited to deal with the variety of problems that exist on the web. These projects have a huge amount of promise but in the meantime it is sometimes more practical to build a custom solution.

## What is a sharding framework?

Twitter has built several custom distributed data-stores. But most of these solutions have a lot in common, prompting us to extract the commonalities so that they were more easily maintainable and reusable in the future. Thus, we have extracted Gizzard, a Scala framework that makes it easy to create custom fault-tolerant, distributed databases.

Gizzard a *framework* in that it offers a basic template for solving a certain class of problem. This template is not perfect for everyone's needs but is useful for a very wide variety of problems.

## How does it work?

### Gizzard is middleware

![Alt text](blob/master/doc//middleware.png?raw=true)

Gizzard operates as a *middleware* networking service. It sits "in the middle" between clients (typically, web front-ends like PHP and Ruby on Rails applications) and the many partitions/replicas of data. Sitting in the middle, all data querying and manipulation flow through Gizzard.

### Gizzard supports any datastorage backend

Gizzard is designed to replicate data across any network-available data storage service. This could be a relational database, Lucene, Redis, or anything you can imagine. As a general rule, Gizzard requires that all write operations be idempotent (see the section on Fault Tolerance and Migrations), so this places some constraints on how you may use the back-end store.

### Gizzard handles partitioning through a forwarding table

Gizzard handles partitioning (i.e., dividing the data across many hosts) by mappings ranges of data to particular shards. These mappings are stored in a table that specifies lower-bound of a numerical range and what partition that data in that range belongs to.

![Alt text](blob/master/doc/forwarding_table.png)

To be precise, you provide Gizzard a custom "hashing" function that, given a key for your data (and this key can be application specific), produces a number that belongs to one of the ranges in the forwarding table. These functions are programmable so you can optimize for locality or balance depending on your needs.

This tabular approach differs from the "consistent hashing" technique used in many other distributed data-stores. This allows for heterogeneously sized partitions so that you manage *hotspots*, segments of data that are extremely popular. In fact, Gizzard does allows you to implement completely custom forwarding strategies including consistent hashing, but this isn't the approach we recommend.

### Gizzard handles replication through a replication tree

Each "shard" referenced in the forwarding table can be either a physical shard or a logical shard. A logical shard is just a tree of other shards, where each *branch* in the tree represents some logical transformation on the data, and each *node* is a "physical" datastorage device. These logical transformations at the branches in the tree are usually rules about how to propagate read and write operations to the children of that branch. For example, here is a two-level replication tree. Note that this represents just ONE partition (as referenced in the forwarding table):

![Alt text](blob/master/doc/replication_tree.png)

The "Replicate" branches in the picture above are simple strategies to repeat write operations to all children and to balance reads across the children according to health and a weighting function. You can create custom branching/logical shards for your particular datastorage needs, such as to add additional transaction/coordination primitives or quorum strategies. But Gizzard ships with a few standard strategies of broad utility such as Replicating, Write-Only, Read-Only, and Blocked (allowing neither reads nor writes). The utility of some of the more obscure shard types is discussed in the section on `Migrations`.

### Gizzard is fault-tolerant

Fault-tolerance is one of the biggest concerns of distributed systems. Because distributed systems involve many computers, there is some likelihood that one (or many) are malfunctioning at any moment. Gizzard is designed to avoid any single points of failure. If any given replica in a shard has crashed, Gizzard routes requests to the remaining healthy replicas. If all replicas of a shard are unavailable, Gizzard will be unable to serve read requests to that shard, but all other shards will be unaffected. Writes to an unavailable shard are buffered until the shard again becomes available.

In fact, if any number of replicas in a shard are unavailable, Gizzard will try to write to all healthy replicas as quickly as possible and buffer the writes to the unavailable shard, to try again later when the unhealthy shard returns. The basic strategy is that all writes are materialized to a durable, transactional message queue. Writes are then performed asynchronously (but with manageably low latency) to all replicas in a shard. If a shard is unavailable, the write operation goes into an error queue and is retried later.

In order to achieve "eventual consistency", this "retry later" strategy requires that your write operations are idempotent. This is because a retry later strategy can apply operations out-of-order. In most cases this is an easy requirement. A demonstration is idempotent writes is given in the Gizzard demo app, `Rowz`.

### Winged migrations

It's sometimes convenient to copy or move data from shards from one computer to another. You might do this to better balance load or to deal with hardware failures. The way this works is that first a replicating shard is created "in front of" the original shard. The new shard is inserted behind the Replicating shard, with a WriteOnly shard in-between. The new shard receives all new writes but does not yet respond to reads since because of the WriteOnly shard. Data from the old shard is then slowly copied to the new shard. When the data is fully copied, the WriteOnly shard is removed and reads can now happen from both the old and new shard. In the case of a "migration", the old shard is removed from the replication tree.

![Alt text](blob/master/doc/migration.png)

Because writes will happen out of order (new writes occur before older ones and some writes may happen twice), all writes must be idempotent.

## How to use Gizzard?

The source-code to Gizzard is [available on GitHub](http://github.com/twitter/gizzard). A sample application that uses Gizzard, called Rowz, [is also available](http://github.com/nkallen/Rowz).

## Installation

### Maven

    <dependency>
        <groupId>com.twitter</groupId>
        <artifactId>gizzard</artifactId>
        <version>1.0</version>
    </dependency>

### Ivy

    <dependency org="com.twitter" name="gizzard" rev="1.0"/>

## Reporting problems

The Github issue tracker is [here](http://github.com/twitter/gizzard/issues).

## Contributors

* Robey Pointer
* Nick Kallen
* Ed Ceaser
* John Kalucki