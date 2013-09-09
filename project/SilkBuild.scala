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


import java.net.InetAddress
import sbt._
import Keys._
import sbtrelease.ReleasePlugin._
import scala.Some
import sbt.ExclusionRule
import xerial.sbt.Pack._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys._
import net.thunderklaus.GwtPlugin._
import com.earldouglas.xsbtwebplugin.PluginKeys._
import com.earldouglas.xsbtwebplugin.Container

object SilkBuild extends Build {

  val SCALA_VERSION = "2.10.2"

  private def profile = System.getProperty("xerial.profile", "default")
  private def isWindows = System.getProperty("os.name").contains("Windows")


  def releaseResolver(v: String): Option[Resolver] = {
    profile match {
      case "default" => {
        val nexus = "https://oss.sonatype.org/"
        if (v.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
      }
      case p => {
        scala.Console.err.println("unknown xerial.profile '%s'".format(p))
        None
      }
    }
  }

  private def junitReport(target:File) = {
    Tests.Argument(TestFrameworks.ScalaTest, "junitxml(directory=\"%s\")".format(target /"test-reports" ), "stdout")
  }

  private def loglevelJVMOpts = {
    import scala.collection.JavaConversions._
    // Pass loglevel options to each JVM instance
    val opts : Seq[String] = (for((k, v) <- System.getProperties if k.startsWith("loglevel")) yield {
      "-D%s=%s".format(k, v)
    }).toSeq
    opts
  }

  lazy val buildSettings = Defaults.defaultSettings ++ releaseSettings ++  SbtMultiJvm.multiJvmSettings ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq[Setting[_]](
    organization := "org.xerial.silk",
    organizationName := "Silk Project",
    organizationHomepage := Some(new URL("http://xerial.org/")),
    description := "Silk: A Scalable Data Processing Platform",
    scalaVersion in Global := SCALA_VERSION,
    sbtVersion in Global := "0.13.0",
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishTo <<= version { v => releaseResolver(v) },
    pomIncludeRepository := {
      _ => false
    },
    logBuffered in Test := false,
    logBuffered in MultiJvm := false,
    testOptions in Test <++= (target in Test) map { target => Seq(junitReport(target), Tests.Filter{name:String => !name.contains("MultiJvm")}) },
    testOptions in MultiJvm <+= (target in MultiJvm) map {junitReport(_)},
    jvmOptions in MultiJvm ++= loglevelJVMOpts,
    scalatestOptions in MultiJvm <++= (target in Compile)( (t: File) => Seq("-u", (t / "test-reports").getAbsolutePath) ),
    //    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
//    executeTests in Test := {
//      val testResults : Tests.Output = (executeTests in Test).value
//      val multiJvmTestResults : Tests.Output = (executeTests in MultiJvm).value
//      val results = testResults.events ++ multiJvmTestResults.events
//      Tests.Output(
//        Tests.overall(Seq(testResults.overall, multiJvmTestResults.overall)),
//        results,
//        testResults.summaries ++ multiJvmTestResults.summaries)
//    },
    unmanagedSourceDirectories in Test <+= (baseDirectory) { _ / "src" / "multi-jvm" / "scala" },
    resolvers ++= Seq(
      //"Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Sonatype shapshot repo" at "https://oss.sonatype.org/content/repositories/snapshots/"
    ),
    parallelExecution in Global := false,
    //parallelExecution in Test := false,
    //parallelExecution in MultiJvm := false,
    crossPaths := false,
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-target:jvm-1.6", "-feature"),
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


  private val dependentScope = "test->test;compile->compile"

  lazy val container = Container("container")

  lazy val root = Project(
    id = "silk",
    base = file("."),
    settings = buildSettings ++ packSettings ++ Seq(
      description := "Silk root project",
      // do not publish the root project
      packExclude := Seq("silk"),
      packMain := Map("silk" -> "xerial.silk.weaver.SilkMain"),
      publish := {},
      publishLocal := {},
      // Disable publishing pom for the root project
      // publishMavenStyle := false,
      // Disable publishing jars for the root project
      //publishArtifact in (Compile, packageBin) := false,
      //publishArtifact in (Compile, packageDoc) := false,
      //publishArtifact in (Compile, packageSrc) := false
      libraryDependencies ++= jettyContainer
  ) ++ container.deploy("/" -> silkWebUI.project)
  ) aggregate(silkCore, silkCluster, silkWebUI, silkWeaver) settings
    (
      addArtifact(Artifact("silk", "arch", "tar.gz"), packArchive).settings:_*
    )



  lazy val silkCore = Project(
    id = "silk-core",
    base = file("silk-core"),
    settings = buildSettings ++ Seq(
      description := "Core library of Silk, a platform for progressive distributed data processing",
      libraryDependencies ++= testLib ++ coreLib ++ Seq(xerialCore, xerialLens, xerialCompress)
    )
  )

  lazy val silkCluster = Project(
    id = "silk-cluster",
    base = file("silk-cluster"),
    settings = buildSettings ++ Seq(
      description := "Silk support of cluster computing",
      libraryDependencies ++= testLib ++ clusterLib ++ shellLib ++ slf4jLib
    )
  ) dependsOn(silkCore % dependentScope)

  lazy val silkWebUI = Project(
    id = "silk-webui",
    base = file("silk-webui"),
    settings = buildSettings ++ gwtSettings ++ Seq(
      description := "Silk Web UI for monitoring node and tasks",
      // Disable publishing the war file, because SilkWebUI can be launched from Jetty using silk-webui.jar
      publishArtifact in (Compile, packageWar) := false,
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
      libraryDependencies ++= webuiLib ++ jettyContainer
    )
  ) dependsOn(silkCluster, silkCore % dependentScope)

  lazy val silkWeaver = Project(
    id = "silk-weaver",
    base = file("silk-weaver"),
    settings = buildSettings ++ Seq(
      description := "Silk Weaver",
      libraryDependencies ++= testLib ++ Seq(xerialCore, xerialLens, xerialCompress)
    )
  ) dependsOn(silkWebUI, silkCluster, silkCore % dependentScope) configs(MultiJvm)


  val copyGWTResources = TaskKey[Unit]("copy-gwt-resources", "Copy GWT resources")


//
//  lazy val xerial = RootProject(file("xerial"))
//  lazy val xerialCore = ProjectRef(file("xerial"), "xerial-core")
//  lazy val xerialLens = ProjectRef(file("xerial"), "xerial-lens")
//  lazy val xerialCompress = ProjectRef(file("xerial"), "xerial-compress")
  //lazy val xerialMacro = ProjectRef(file("xerial"), "xerial-macro")


  val AKKA_VERSION = "2.1.4"
  val XERIAL_VERSION = "3.2.1"

  object Dependencies {

    val xerialCore = "org.xerial" % "xerial-core" % XERIAL_VERSION
    val xerialLens = "org.xerial" % "xerial-lens" % XERIAL_VERSION
    val xerialCompress = "org.xerial" % "xerial-compress" % XERIAL_VERSION


    val testLib = Seq(
      "junit" % "junit" % "4.10" % "test",
      "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test",
      "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test",
      "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION % "test"
      //"com.typesafe.akka" %% "akka-remote-tests-experimental" % "2.1.2" % "test"
    )

    val shellLib = Seq(
      "org.fusesource.jansi" % "jansi" % "1.10",
      "org.scala-lang" % "jline" % SCALA_VERSION
    )


    val coreLib = Seq(
      "org.xerial" % "larray" % "0.1.2",
      "org.ow2.asm" % "asm-all" % "4.1",
      "org.scala-lang" % "scalap" % SCALA_VERSION,
      "org.scala-lang" % "scala-reflect" % SCALA_VERSION
    )

    val zkLib = Seq(
      "org.apache.zookeeper" % "zookeeper" % "3.4.5" excludeAll(
        ExclusionRule(organization="org.jboss.netty"),
        ExclusionRule(organization="com.sun.jdmk"),
        ExclusionRule(organization="com.sun.jmx"),
        ExclusionRule(organization="javax.jms"),
        ExclusionRule(organization="org.slf4j")
        ),
      "com.netflix.curator" % "curator-recipes" % "1.3.3" excludeAll(
        ExclusionRule(organization="org.slf4j")
        ),
      "com.netflix.curator" % "curator-test" % "1.3.3" excludeAll(
        ExclusionRule(organization="org.slf4j")
        )
    )

    val slf4jLib = Seq(
      "org.slf4j" % "slf4j-api" % "1.6.4",
      "org.slf4j" % "slf4j-log4j12" % "1.6.4",
      "log4j" % "log4j" % "1.2.16"
    )

    val clusterLib = zkLib ++ slf4jLib ++ Seq(
      //"io.netty" % "netty" % "3.6.1.Final",
      "org.xerial.snappy" % "snappy-java" % "1.1.0-M4",
      "com.typesafe.akka" %% "akka-actor" % AKKA_VERSION,
      "com.typesafe.akka" %% "akka-remote" % AKKA_VERSION,
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "com.esotericsoftware.kryo" % "kryo" % "2.20" excludeAll (
          ExclusionRule(organization="org.ow2.asm")
        )
    )


    val JETTY_VERSION = "7.0.2.v20100331" // "9.0.5.v20130815" //  //"8.1.11.v20130520"
    val GWT_VERSION = "2.5.1"

    // We need to use an older version of jetty because newer version of jetty embeds ASM3 library,
    // which conflicts with ASM4 used in ClosureSerializer
    val jettyContainer = Seq("org.mortbay.jetty" % "jetty-runner" % JETTY_VERSION % "container" )

    val excludeSlf4j = ExclusionRule(organization = "org.slf4j")

    val webuiLib = slf4jLib ++ Seq(
      "org.mortbay.jetty" % "jetty-runner" % JETTY_VERSION excludeAll (
        // Exclude JSP modules if necessary
        ExclusionRule(organization="org.mortbay.jetty", name="jsp-2.1-glassfish"),
        ExclusionRule(organization="org.eclipse.jdtj"),
        ExclusionRule(organization = "org.slf4j")
        ),
      "com.google.gwt" % "gwt-user" % GWT_VERSION % "provided",
      "com.google.gwt" % "gwt-dev" % GWT_VERSION % "provided",
      "com.google.gwt" % "gwt-servlet" % GWT_VERSION % "runtime",
      "org.fusesource.scalate" % "scalate-core_2.10" % "1.6.1" excludeAll (
        ExclusionRule(organization="org.slf4j"),
        ExclusionRule(organization="org.scala-lang")
        )
    )

  }

}








