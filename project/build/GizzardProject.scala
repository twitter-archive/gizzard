import sbt._
import com.twitter.sbt._

class GizzardProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher with InlineDependencies {
  override def filterScalaJars = false
  val scalaTools = "org.scala-lang" % "scala-compiler" % "2.7.7"

  inline("com.twitter" %% "querulous" % "1.5.0")
  inline("net.lag" % "configgy" % "1.6.8")
  inline("net.lag" % "kestrel" % "1.3-config-SNAPSHOT")
  inline("com.twitter" % "ostrich" % "1.2.10")
  inline("com.twitter" % "util" % "1.1.2")

  val rpcclient = "com.twitter" %% "rpcclient" % "1.1.2-SNAPSHOT"
  val slf4j     = "org.slf4j" % "slf4j-jdk14" % "1.5.2"
  val slf4jApi  = "org.slf4j" % "slf4j-api" % "1.5.2"
  val thrift    = "thrift" % "libthrift" % "0.5.0"
  val json      = "com.twitter" %% "json" % "2.1.5"
  val jackson   = "org.codehaus.jackson" % "jackson-core-asl" % "1.6.1"
  val jacksonMap = "org.codehaus.jackson" % "jackson-mapper-asl" % "1.6.1"

  // test jars
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
