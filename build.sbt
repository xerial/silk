
organization := "org.xerial.silk"
sonatypeProfileName := "org.xerial"
name := "silk"
description := "A framework for simplifying SQL pipelines"
scalaVersion := "2.11.7"

lazy val root = project.in(file(".")).aggregate(core, framework)

lazy val core = project in file("silk-core").settings(
  libraryDependencies ++= Seq(
    "com.github.nscala-time" %% "nscala-time" % "2.0.0",
    "org.xerial" % "xerial-lens" % "3.3.6",
    "org.scala-lang" % "scalap" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )
)

lazy val framework = project in file("silk-framework") dependsOn core