
organization := "org.xerial.silk"
sonatypeProfileName := "org.xerial"
description := "A framework for simplifying SQL pipelines"
scalaVersion in Global := "2.11.7"

packSettings
packMain := Map("silk" -> "xerial.silk.cui.SilkMain")
packExclude := Seq("silk-root")

lazy val root = (project in file(".")).settings(
  name := "silk-root",
  publish := {}
).aggregate(core, cui)

lazy val core = (project in file("silk-core")).settings(
  name := "silk-core",
  libraryDependencies ++= Seq(
    "com.github.nscala-time" %% "nscala-time" % "2.0.0",
    "org.xerial" % "xerial-lens" % "3.3.8",
    "org.scala-lang" % "scalap" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.ow2.asm" % "asm-all" % "4.1",
    "com.esotericsoftware.kryo" % "kryo" % "2.20" exclude("org.ow2.asm", "asm"),
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )
)

lazy val cui = project.in(file("silk-cui")).settings(
  name := "silk-cui"
).dependsOn(core)

pomExtra in Global := {
    <url>http://xerial.org/silk</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/xerial/silk.git</connection>
      <developerConnection>scm:git:git@github.com:xerial/silk.git</developerConnection>
      <url>github.com/xerial/silk.git</url>
    </scm>
    <properties>
      <scala.version>{scalaVersion.value}</scala.version>
      <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <developers>
      <developer>
        <id>leo</id>
        <name>Taro L. Saito</name>
        <url>http://xerial.org/leo</url>
      </developer>
    </developers>
}
