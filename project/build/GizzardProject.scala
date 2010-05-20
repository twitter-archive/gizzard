import sbt._
import com.twitter.sbt.StandardProject


class GizzardProject(info: ProjectInfo) extends StandardProject(info) {
  val asm       = "asm" % "asm" %  "1.5.3"
  val cglib     = "cglib" % "cglib" % "2.1_3"
  val configgy  = "net.lag" % "configgy" % "1.5.4"
  val dbcp      = "commons-dbcp" % "commons-dbcp" % "1.2.2"
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1"
  val jmock     = "org.jmock" % "jmock" % "2.4.0"
  val kestrel   = "net.lag" % "kestrel" % "1.2"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.6"
  val objenesis = "org.objenesis" % "objenesis" % "1.1"
  val ostrich   = "com.twitter" % "ostrich" % "1.1.18"
  val pool      = "commons-pool" % "commons-pool" % "1.3"
  val querulous = "com.twitter" % "querulous" % "1.1.10"
  val slf4j     = "org.slf4j" % "slf4j-jdk14" % "1.5.2"
  val slf4jApi  = "org.slf4j" % "slf4j-api" % "1.5.2"
  val specs     = "org.scala-tools.testing" % "specs" % "1.6.2.1"
  val thrift    = "thrift" % "libthrift" % "0.2.0"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"

  val publishTo = Resolver.sftp("green.lag.net", "green.lag.net", "/web/nest")
}
