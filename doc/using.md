# How to use gizzard

The source-code to Gizzard is on github: [http://github.com/twitter/gizzard](http://github.com/twitter/gizzard).

A sample application that uses Gizzard, called Rowz, is also available:
[http://github.com/twitter/Rowz](http://github.com/twitter/Rowz). The best way
to get started with Gizzard is to clone Rowz and customize.

## Building

We use [sbt](http://code.google.com/p/simple-build-tool/) to build:

    $ sbt clean update package-dist

but there are some pre-requisites. You need:

- java 1.6
- sbt 0.7.4
- thrift 0.2.0

If you haven't used sbt before, this page has a quick setup:
[http://code.google.com/p/simple-build-tool/wiki/Setup](http://code.google.com/p/simple-build-tool/wiki/Setup).
My `~/bin/sbt` looks like this:

    #!/bin/bash
    java -server -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -Xmx1024m -jar `dirname $0`/sbt-launch-0.7.4.jar "$@"

Apache Thrift 0.5.0 is pre-requisite for building java stubs of the thrift
IDL. It can't be installed via jar, so you'll need to install it separately
before you build. It can be found on the apache incubator site:
[http://incubator.apache.org/thrift/](http://incubator.apache.org/thrift/).

In addition, the tests require a local MySQL instance to be running, and for
`DB_USERNAME` and `DB_PASSWORD` environment variables to contain login info
for it.

The sbt build should download any missing jars using ivy (which is ant's
implementation of the maven dependency fetcher) and build a jar in `dist/`.

## Installation

### Maven

    <dependency>
        <groupId>com.twitter</groupId>
        <artifactId>gizzard</artifactId>
        <version>2.1.6</version>
    </dependency>

It may require you to add the "scala-tools" maven repo to your repo list. The
nest repo is located here:

    http://scala-tools.org/repo-releases/

### Sbt

    val gizzard = "com.twitter" % "gizzard" % "2.1.6"

You will need to add a reference to the "scala-tools" repo if it isn't already
there:

    val scalaToolsReleases = "scala-tools.org" at "http://scala-tools.org/repo-releases/"

## Development

The Github issue tracker is [here](http://github.com/twitter/gizzard/issues).

The google-groups mailing list is [here](http://groups.google.com/group/gizzard).
