import sbt._
import com.twitter.sbt._

class GizzardProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher with InlineDependencies {
  override def filterScalaJars = false
  val scalaTools  = buildScalaVersion match {
    case "2.7.7" => "org.scala-lang" % "scala-compiler" % "2.7.7"
    case _       => "org.scala-lang" % "scala-compiler" % "2.8.1"
  }

  val querulous   = buildScalaVersion match {
    case "2.7.7" => "com.twitter" % "querulous_2.7.7" % "1.5.4"
    case _       => "com.twitter" % "querulous_2.8.0" % "1.5.4"
  }

  val configgy    = buildScalaVersion match {
    case "2.7.7" => "net.lag"     % "configgy" % "1.6.10"
    case _       => "net.lag"     % "configgy" % "2.0.2"
 }

 //val kestrel     = "net.lag" % "kestrel" % "1.2.7"
  // remove when moved to libkestrel
  val twitterActors = "com.twitter" % "twitteractors_2.8.0" % "2.0.1"

  val ostrich     = buildScalaVersion match {
    case "2.7.7" => "com.twitter" % "ostrich"  % "1.2.15"
    case _       => "com.twitter" % "ostrich"  % "2.3.6"
  }

  val util = buildScalaVersion match {
    case "2.7.7" => "com.twitter" % "util" % "1.1.2"
    case _       => "com.twitter" % "util" % "1.6.4"
  }

  val rpcclient = buildScalaVersion match {
    case "2.7.7" => "com.twitter" % "rpcclient_2.7.7" % "1.2.0-gizzardRC1"
    case _       => "com.twitter" % "rpcclient_2.8.0" % "1.2.0-gizzardRC1"
  }

  val thrift      = "thrift" % "libthrift" % "0.5.0"
  val jackson     = "org.codehaus.jackson" % "jackson-core-asl"   % "1.6.1"
  val jacksonMap  = "org.codehaus.jackson" % "jackson-mapper-asl" % "1.6.1"

  val slf4j       = "org.slf4j" % "slf4j-jdk14" % "1.5.2" //intransitive
  val slf4jApi    = "org.slf4j" % "slf4j-api"   % "1.5.2" //intransitive

  // test jars

  val specs     = buildScalaVersion match {
    case "2.7.7" => "org.scala-tools.testing" % "specs"       % "1.6.2.1" % "test"
    case _       => "org.scala-tools.testing" % "specs_2.8.0" % "1.6.5"   % "test"
  }

  val objenesis = "org.objenesis" % "objenesis"    % "1.1"    % "test"
  val jmock     = "org.jmock"     % "jmock"        % "2.4.0"  % "test"
  val hamcrest  = "org.hamcrest"  % "hamcrest-all" % "1.1"    % "test"
  val asm       = "asm"           % "asm"          %  "1.5.3" % "test"
  val cglib     = "cglib"         % "cglib"        % "2.1_3"  % "test"

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
