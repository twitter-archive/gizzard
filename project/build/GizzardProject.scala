import sbt._
import com.twitter.sbt._


class GizzardProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher {
  val configgy  = "net.lag" % "configgy" % "1.6.1"
  val dbcp      = "commons-dbcp" % "commons-dbcp" % "1.2.2"
  val kestrel   = "net.lag" % "kestrel" % "1.2"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.6"
  val ostrich   = "com.twitter" % "ostrich" % "1.2.2"
  val pool      = "commons-pool" % "commons-pool" % "1.3"
  val querulous = "com.twitter" % "querulous" % "1.1.13"
  val slf4j     = "org.slf4j" % "slf4j-jdk14" % "1.5.2"
  val slf4jApi  = "org.slf4j" % "slf4j-api" % "1.5.2"
  val thrift    = "thrift" % "libthrift" % "0.2.0"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"
  val json      = "com.twitter" % "json" % "1.1.4"

  val specs     = "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val jmock     = "org.jmock" % "jmock" % "2.4.0" % "test"
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
  val asm       = "asm" % "asm" %  "1.5.3" % "test"
  val cglib     = "cglib" % "cglib" % "2.1_3" % "test"

  override def pomExtra =
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")
}
