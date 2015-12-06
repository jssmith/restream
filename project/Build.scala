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
      "-target:jvm-1.8",
      "-feature",
      "-language:implicitConversions",
      "-language:experimental.macros",
      "-deprecation"
    ),
    fork in Test := true,
    testForkedParallel := false,
    javaOptions in Test ++= Seq("-Xmx6G", "-Xms6G"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    test in assembly := {}
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
        "com.twitter" %% "chill" % "0.7.2",
        "io.netty" % "netty" % "3.10.5.Final",
        "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
        "org.slf4j" % "slf4j-simple" % "1.7.13",
        "org.apache.commons" % "commons-math3" % "3.5",
        "com.google.guava" % "guava" % "18.0",
        "org.scalatest" %% "scalatest" % "2.2.5"
      )
    )
  )

  lazy val apps = Project("apps", file("apps"),
    settings = buildSettings ++ Seq(
      name := "replaydb-apps",
      libraryDependencies ++= Seq(
      // TODO why do I need to include this again? Why isn't it picked up from core?
        "io.netty" % "netty" % "3.10.5.Final"
      )
    )
  ) dependsOn (core)

  lazy val spark_apps = Project("spark-apps", file("spark-apps"),
    settings = buildSettings ++ Seq(
      name := "replaydb-spark-apps",
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
        "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided",
        "org.apache.spark" %% "spark-hive" % "1.5.1" % "provided"
      )
    )
  ) dependsOn (core)
}