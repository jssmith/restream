import sbt._
import sbt.Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._

object BuildSettings {
  val buildSettings = Defaults.coreDefaultSettings ++ Seq(
    organization := "replaydb",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq(
      "-feature",
      "-language:implicitConversions",
      "-language:experimental.macros",
      "-deprecation"
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
}

object ReplayDBBuild extends Build {
  import BuildSettings._

  lazy val root = Project("root", file("."),
    settings = buildSettings
  ) aggregate(core, apps)

  lazy val core = Project("core", file("core"),
    settings = buildSettings ++ Seq(
      name := "replaydb-core",
      libraryDependencies ++= Seq(
        "com.twitter" %% "chill" % "0.7.1",
        "org.apache.commons" % "commons-math3" % "3.5",
        "com.google.guava" % "guava" % "18.0",
        "org.scalatest" %% "scalatest" % "2.2.5"
      )
    )
  )

  lazy val apps = Project("apps", file("apps"),
    settings = buildSettings ++ Seq(
      name := "replaydb-apps"
    )
  ) dependsOn (core)
}