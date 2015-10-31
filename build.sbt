name := "replaydb"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-feature",
  "-language:implicitConversions",
  "-language:experimental.macros",
  "-deprecation"
)

libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-reflect" % _)

libraryDependencies ++= Seq(
  "com.twitter" %% "chill" % "0.7.1",
  "org.apache.commons" % "commons-math3" % "3.5",
  "com.google.guava" % "guava" % "18.0",
  "org.scalatest" %% "scalatest" % "2.2.5"
)