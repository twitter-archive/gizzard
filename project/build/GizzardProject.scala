import sbt._
import com.twitter.sbt._

class GizzardProject(info: ProjectInfo) extends StandardLibraryProject(info)
with CompileThriftFinagle
with DefaultRepos
with SubversionPublisher 
with ProjectDependencies {

  def finagleVersion = "3.0.0"
  def utilVersion = "3.0.0"

  override def filterScalaJars = false
  val scalaTools = "org.scala-lang" % "scala-compiler" % "2.8.1"

  val querulous  = "com.twitter" % "querulous-core" % "2.7.6"

  //val kestrel     = "net.lag" % "kestrel" % "1.2.7"
  // remove when moved to libkestrel
  val twitterActors = "com.twitter" % "twitteractors_2.8.0" % "2.0.1"

  val finagleThrift   = "com.twitter"          % "finagle-thrift"     % finagleVersion
  val finagleOstrich4 = "com.twitter"          % "finagle-ostrich4"   % finagleVersion

  val utilThrift      = "com.twitter"          % "util-thrift"        % utilVersion
  val jackson         = "org.codehaus.jackson" % "jackson-core-asl"   % "1.9.2"
  val jacksonMap      = "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.2"

  val slf4j      = "org.slf4j" % "slf4j-jdk14" % "1.5.2" //intransitive
  val slf4jApi   = "org.slf4j" % "slf4j-api"   % "1.5.2" //intransitive

  // test jars

  val specs     = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" withSources()
  val objenesis = "org.objenesis" % "objenesis"    % "1.1"    % "test"
  val jmock     = "org.jmock"     % "jmock"        % "2.4.0"  % "test"
  val hamcrest  = "org.hamcrest"  % "hamcrest-all" % "1.1"    % "test"
  val asm       = "asm"           % "asm"          % "1.5.3"  % "test"
  val cglib     = "cglib"         % "cglib"        % "2.2"    % "test"

  override def pomExtra =
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>

  override def subversionRepository = Some("https://svn.twitter.biz/maven-public")

  /* We need stuff from the config directory to run tests in a invocation-path
   * independent manner.
   */
  override def testResources = 
    descendents(testResourcesPath ##, "*") +++
    descendents(info.projectPath / "config" ##, "*")
}
