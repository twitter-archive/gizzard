import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val scalaTools = "twitter.com" at "http://maven.twttr.com/"
//  val lagNet = "twitter.com" at "http://www.lag.net/repo/"
  val defaultProject = "com.twitter" % "standard-project" % "0.7.5"
}
