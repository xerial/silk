/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.net.{URL, InetAddress}
import sbt._
import complete.DefaultParsers._
import Keys._
import sbtrelease.ReleasePlugin._
import sbt.ExclusionRule
import xerial.sbt.Pack._
import net.thunderklaus.GwtPlugin._
import com.earldouglas.xsbtwebplugin.PluginKeys._
import com.earldouglas.xsbtwebplugin.Container
import sbt.ScriptedPlugin._

object SilkBuild extends Build {

  val SCALA_VERSION = "2.11.6"

  val silkRun = InputKey[Unit]("silk-run", "run silk workflow")

  private def junitReport(target:File) = {
    Tests.Argument(TestFrameworks.ScalaTest, "-u", s"${target /"test-reports"}", "-o")
  }

  private def loglevelJVMOpts = {
    import scala.collection.JavaConversions._
    // Pass loglevel options to each JVM instance
    val opts : Seq[String] = (for((k, v) <- System.getProperties if k.startsWith("loglevel")) yield {
      "-D%s=%s".format(k, v)
    }).toSeq
    opts
  }

  lazy val buildSettings = Defaults.coreDefaultSettings ++ releaseSettings  ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq[Setting[_]](
    organization := "org.xerial.silk",
    organizationName := "Silk Project",
    organizationHomepage := Some(new URL("http://xerial.org/")),
    description := "Silk: A Scalable Data Processing Platform",
    scalaVersion in Global := SCALA_VERSION,
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := {
      _ => false
    },
    logBuffered in Test := false,
    testOptions in Test <++= (target in Test) map { target => Seq(junitReport(target), Tests.Filter{name:String => !name.contains("MultiJvm")}) },
    resolvers ++= Seq(
      //"Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Sonatype shapshot repo" at "https://oss.sonatype.org/content/repositories/snapshots/"
    ),
    //parallelExecution in Test := false,
    crossPaths := false,
    // Since sbt-0.13.2
    incOptions := incOptions.value.withNameHashing(true),
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-target:jvm-1.8", "-feature"),
    pomExtra := {
      <url>http://xerial.org/</url>
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
          <scala.version>{SCALA_VERSION}</scala.version>
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
  )

  import Dependencies._


  private val withTestScope = "test->test;compile->compile"

  lazy val container = Container("container")

  lazy val root = Project(
    id = "silk",
    base = file("."),
    settings =
      buildSettings
      ++ packSettings
      ++ Seq(
        description := "Silk root project",
        silkRun := {
          val logger = streams.value.log
          val args = spaceDelimited("<arg>").parsed
          logger.info(s"run silk workflow: args ${args.mkString(", ")}")
        },
        libraryDependencies ++= jettyContainer,
        packExclude := Seq("silk", "silk-sbt"),
        packMain := Map("silk" -> "xerial.silk.weaver.SilkMain"),
        publishLocalConfiguration ~= { config =>
          // Publish only tar.gz archive. Do not publish pom, jar and sources for the root project.
          val m = config.artifacts.filter(_._1.`type` == "arch")
          new PublishConfiguration(config.ivyFile, config.resolverName, m, config.checksums, config.logging)
        }
      )
      ++ publishPackTgzArchive
      ++ container.deploy("/" -> silkWebUI.project)
  ) aggregate(silkCore, silkCui, silkFramework, silkSigar, silkWebUI) // Add silkSbt when sbt support Scala 2.11 plugin


  lazy val silkCore = Project(
    id = "silk-core",
    base = file("silk-core"),
    settings = buildSettings ++ Seq(
      description := "Silk core operations and utilities",
      libraryDependencies ++= testLib
        ++ Seq(xerialCore, xerialLens, xerialCompress)
        ++ slf4jLib
    )
  )

  lazy val silkCui = Project(
    id = "silk-cui",
    base = file("silk-cui"),
    settings = buildSettings ++ Seq(
       description := "Silk command line tool",
       libraryDependencies ++= testLib
    )
  ) dependsOn(silkFramework, silkCore % withTestScope)

  lazy val silkSigar = Project(
    id = "silk-sigar",
    base = file("silk-sigar"),
    settings = buildSettings ++ Seq(
       description := "Sigar library wrapper",
       libraryDependencies ++= sigarLib
    )
  ) dependsOn(silkCore % "test->test")

  lazy val silkFramework = Project(
    id = "silk-framework",
    base = file("silk-framework"),
    settings = buildSettings ++ Seq(
      description := "Silk Framework",
      libraryDependencies ++= frameworkLib ++ shellLib ++ akkaLib
    )
  ) dependsOn(silkCore % withTestScope)


  lazy val silkWebUI = Project(
    id = "silk-webui",
    base = file("silk-webui"),
    settings = buildSettings
      ++ gwtSettings
      ++ Seq(
      description := "Silk Web UI",
      // Publish the jar file so that silk-cluster.jar file can be found in classpaths
      publishArtifact in (Compile, packageBin) := true,
      // Disable publishing .war file
      packagedArtifacts <<= packagedArtifacts map { as => as.filter(_._1.`type` != "war") },
      gwtVersion := GWT_VERSION,
      //gwtModules := Seq("xerial.silk.webui.Silk"),
      gwtBindAddress := {
        if(sys.props.contains("gwt.expose")) Some(InetAddress.getLocalHost.getHostAddress) else None
      },
      gwtForceCompile := false,
      packageBin in Compile <<= (packageBin in Compile).dependsOn(copyGWTResources),
      javaOptions in Gwt in Compile ++= Seq(
        "-strict", "-Xmx1g"
      ),
      javaOptions in Gwt ++= Seq(
        "-Xmx1g", "-Dloglevel=debug", "-Dgwt-hosted-mode=true"
      ),
      webappResources in Compile <+= (resourceDirectory in Compile)(d => d / "xerial/silk/webui/webapp"),
      copyGWTResources := {
        val gwtCompileResult = gwtCompile.value // invoke gwtCompile
        val gwtOut = gwtTemporaryPath.value
        val s = streams.value
        val output = target.value / "classes/xerial/silk/webui/webapp"
        val bd = baseDirectory.value
        def rpath(path:File) =  path.relativeTo(bd).getOrElse(path)

        s.log.info("copy GWT output " + rpath(gwtOut) + " to " + rpath(output))
        val p = (gwtOut ** "*") --- (gwtOut / "WEB-INF" ** "*")

        for(file <- p.get; relPath <- file.relativeTo(gwtOut)) {
          val out = output / relPath.getPath
          if(file.isDirectory) {
            s.log.info("create direcotry: " + rpath(out))
            IO.createDirectory(out)
          }
          else {
            s.log.info("copy " + rpath(file) + " to " + rpath(out))
            IO.copyFile(file, out, preserveLastModified=true)
          }
        }
      },
      libraryDependencies ++= testLib ++ shellLib ++ webuiLib ++ jettyContainer ++ Seq(xerialCore, xerialLens, xerialCompress)
    )
  ) dependsOn(silkFramework, silkSigar, silkCore  % withTestScope)

  // sbt plugin project
  lazy val silkSbt = Project(
    id = "silk-sbt",
    base = file("silk-sbt"),
    settings = Defaults.coreDefaultSettings ++ releaseSettings ++ scriptedSettings ++ Seq(
      organization := "org.xerial.sbt",
      organizationName := "Xerial project",
      organizationHomepage := Some(new URL("http://xerial.org/")),
      description := "A sbt plugin for developing Silk programs",
      scalaVersion in Global := "2.10.5", // We need to use Scala 2.10 for sbt plugins
      sbtVersion in Global := "0.13.8",
      publishMavenStyle := true,
      publishArtifact in Test := false,
      pomIncludeRepository := {
        _ => false
      },
      sbtPlugin := true,
      parallelExecution := true,
      crossPaths := false,
      scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-target:jvm-1.6"),
      scriptedBufferLog := false,
      scriptedLaunchOpts ++= {
        import scala.collection.JavaConverters._
        management.ManagementFactory.getRuntimeMXBean().getInputArguments().asScala.filter(a => Seq("-Xmx","-Xms").contains(a) || a.startsWith("-XX")).toSeq
      },
      libraryDependencies ++= Seq(xerialCore)
    )
  )


  val copyGWTResources = TaskKey[Unit]("copy-gwt-resources", "Copy GWT resources")


  val AKKA_VERSION = "2.3.9"
  val XERIAL_VERSION = "3.3.6"


  object Dependencies {

    val xerialCore = "org.xerial" % "xerial-core" % XERIAL_VERSION
    val xerialLens = "org.xerial" % "xerial-lens" % XERIAL_VERSION
    val xerialCompress = "org.xerial" % "xerial-compress" % XERIAL_VERSION


    val testLib = Seq(
      "junit" % "junit" % "4.10" % "test",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.12.1" % "test",
      "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION % "test"
      //"com.typesafe.akka" %% "akka-remote-tests-experimental" % "2.1.2" % "test"
    )

    val shellLib = Seq(
      "org.fusesource.jansi" % "jansi" % "1.10",
      "jline" % "jline" % "2.12"
    )


    val frameworkLib = Seq(
      "org.xerial" % "larray" % "0.1.1",
      "org.ow2.asm" % "asm-all" % "4.1",
      "org.scala-lang" % "scalap" % SCALA_VERSION,
      "org.scala-lang" % "scala-reflect" % SCALA_VERSION,
      "com.esotericsoftware.kryo" % "kryo" % "2.20"
        exclude("org.ow2.asm", "asm"),
      "com.google.protobuf" % "protobuf-java" % "2.4.1"
    )

    val hadoopLib = Seq(
      "org.apache.hadoop" % "hadoop-common" % "2.2.0"
        exclude("org.slf4j", "slf4j-api")
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("asm", "asm")
        exclude("com.google.protobuf", "protobuf-java"),
      "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0"
        exclude("com.google.protobuf", "protobuf-java")
    )

    val zkLib = Seq(
      "org.apache.zookeeper" % "zookeeper" % "3.4.5"
        exclude("org.slf4j", "slf4j-api")
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("org.jboss.netty", "netty"),
      "com.netflix.curator" % "curator-recipes" % "1.3.3"
        exclude("org.slf4j", "slf4j-api")
      ,
      "com.netflix.curator" % "curator-test" % "1.3.3"
    )

    val slf4jLib = Seq(
      "org.slf4j" % "slf4j-api" % "1.6.4",
      "org.slf4j" % "slf4j-log4j12" % "1.6.4",
      "log4j" % "log4j" % "1.2.16"
    )

    val akkaLib = Seq(
      "com.typesafe.akka" %% "akka-actor" % AKKA_VERSION
    )

    val sigarLib = Seq(
      "org.fusesource" % "sigar" % "1.6.4"
    )

    val clusterLib = zkLib ++ slf4jLib ++ Seq(
      //"io.netty" % "netty" % "3.6.1.Final",
      "com.typesafe.akka" %% "akka-remote" % AKKA_VERSION,
      "org.xerial.snappy" % "snappy-java" % "1.1.0.6"
    )

    val JETTY_VERSION = "7.0.2.v20100331" // "9.0.5.v20130815" //  //"8.1.11.v20130520"
    val GWT_VERSION = "2.5.1"

    // We need to use an older version of jetty because newer version of jetty embeds ASM3 library,
    // which conflicts with ASM4 used in ClosureSerializer
    val jettyContainer = Seq("org.mortbay.jetty" % "jetty-runner" % JETTY_VERSION % "container" )

    val excludeSlf4j = ExclusionRule(organization = "org.slf4j")

    val webuiLib = slf4jLib ++ Seq(
      "org.mortbay.jetty" % "jetty-runner" % JETTY_VERSION
        // Exclude JSP modules if necessary
        exclude("org.mortbay.jetty", "jsp-2.1-glassfish")
        exclude("org.slf4j", "slf4j-api"),
        //exclude("org.eclipse.jdtj"),
      "com.google.gwt" % "gwt-user" % GWT_VERSION % "provided",
      "com.google.gwt" % "gwt-dev" % GWT_VERSION % "provided",
      "com.google.gwt" % "gwt-servlet" % GWT_VERSION % "runtime",
      "org.scalatra.scalate" %% "scalate-core" % "1.7.0"
        exclude("org.slf4j", "slf4j-api")
        exclude("org.scala-lang", "scala-compiler")
        exclude("org.scala-lang", "scala-reflect")
    )

  }

}








