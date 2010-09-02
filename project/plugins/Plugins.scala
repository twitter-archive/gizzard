import sbt._


class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val scalaToolsReleases = "scala-tools.org" at "http://scala-tools.org/repo-releases/"
  val twitterGithubRepository = "twitter.com" at "http://twitter.github.com/repo/"
  val twitterNestRepository = "twitter.com" at "http://www.lag.net/nest"
  val defaultProject = "com.twitter" % "standard-project" % "0.7.0"
}
