name := "preflow"

showSuccess := false
onLoadMessage := ""

logLevel in run := Level.Error

version := "1.0"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.6.14"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
"com.typesafe.akka" %% "akka-actor-typed" % akkaVersion)
