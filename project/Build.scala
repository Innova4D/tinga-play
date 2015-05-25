import sbt._
import Keys._

object ApplicationBuild extends Build {

  val appName = "tinga-play"
  val appVersion = "1.0-Snapshot"

  scalaVersion := "2.11.6"

  ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

  resolvers += Resolver.file("Local repo", file(System.getProperty("user.home") + "/.ivy2/local"))(Resolver.ivyStylePatterns)

  val appDependencies = Seq(
    "org.reactivemongo" %% "play2-reactivemongo" % "0.10.5.0.akka23",
    "com.vidtecci" % "tinga_2.11" % "0.1.0"
    )

  val main = Project(appName, file(".")).enablePlugins(play.PlayScala).settings(
    version := appVersion,
    libraryDependencies ++= appDependencies
  )

}
