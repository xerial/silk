package net.thunderklaus

import sbt._
import sbt.Keys._
import java.io.File
import com.earldouglas.xsbtwebplugin.PluginKeys._
import com.earldouglas.xsbtwebplugin.Container


object GwtPlugin extends Plugin {

  lazy val Gwt = config("gwt") extend (Compile)

  val gwtModules = TaskKey[Seq[String]]("gwt-modules")
  val gwtCompile = TaskKey[Int]("gwt-compile", "Runs the GWT compiler")
  val gwtForceCompile = TaskKey[Boolean]("gwt-force-compile", "Always recompile gwt modules")
  val gwtDevMode = TaskKey[Unit]("gwt-devmode", "Runs the GWT devmode shell")
  val gwtSuperDevMode = TaskKey[Int]("gwt-superdev", "Runs the GWT super devmode code server")
  val gwtVersion = SettingKey[String]("gwt-version")
  val gwtTemporaryPath = SettingKey[File]("gwt-temporary-path")
  val gwtDevTemporaryPath = SettingKey[File]("gwt-dev-temporary-path")
  val gwtWebappPath = SettingKey[File]("gwt-webapp-path")
  val gaeSdkPath = SettingKey[Option[String]]("gae-sdk-path")
  val gwtBindAddress = SettingKey[Option[String]]("gwt-bind-address")

  var gwtModule: Option[String] = None
//  val gwtSetModule = Command.single("gwt-set-module") { (state, arg) =>
//    Project.runTask(gwtModules, state) match {
//      case Some(Value(mods)) => {
//        gwtModule = mods.find(_.toLowerCase.contains(arg.toLowerCase))
//        gwtModule match {
//          case Some(m) => println("gwt-devmode will run: " + m)
//          case None => println("No match for '" + arg + "' in " + mods.mkString(", "))
//        }
//      }
//      case _ => None
//    }
//    state
//  }

  lazy val gwtSettings: Seq[Setting[_]] = com.earldouglas.xsbtwebplugin.WebPlugin.webSettings ++ gwtOnlySettings

  lazy val gwtOnlySettings: Seq[Setting[_]] = inConfig(Gwt)(Defaults.configSettings) ++ Seq(

    managedClasspath in Gwt <<= (managedClasspath in Compile, update) map {
      (cp, up) => cp ++ Classpaths.managedJars(Provided, Set("src"), up)
    },
    unmanagedClasspath in Gwt := { (unmanagedClasspath in Compile).value },
    gwtTemporaryPath <<= (target) { (target) =>
      val t = target / "gwt"
      t.mkdirs()
      t
    },
    gwtDevTemporaryPath <<= (target) { (target) =>
      val t = target / "gwt-dev"
      t.mkdirs()
      t
    },
    gwtBindAddress := None,
    gwtWebappPath <<= (baseDirectory) { (bd) => bd / "war" },
    gwtVersion := "2.5.1",
    gwtForceCompile := false,
    gaeSdkPath := None,
    libraryDependencies <++= gwtVersion{ v =>
      val s = Seq(
        "com.google.gwt" % "gwt-user" % v % "provided",
        "com.google.gwt" % "gwt-dev" % v % "provided",
        "javax.validation" % "validation-api" % "1.0.0.GA" % "provided" withSources (),
        "com.google.gwt" % "gwt-servlet" % v)

      import Version._
      v match {
        case Version(VersionInfo(major, minor, _)) if major >=2 && minor >= 5 =>
          s :+ "com.google.gwt" % "gwt-codeserver" % v % "provided"
        case _ =>
          s
      }
    },
    gwtModules <<= (javaSource in Compile, resourceDirectory in Compile) map {
      (javaSource, resources) => findGwtModules(javaSource) ++ findGwtModules(resources)
    },
    gwtDevMode <<= (dependencyClasspath in Gwt, thisProject in Gwt,  state in Gwt, javaSource in Compile, javaOptions in Gwt,
                    gwtModules, gaeSdkPath, gwtWebappPath, streams) map {
      (dependencyClasspath, thisProject, pstate, javaSource, javaOpts, gwtModules, gaeSdkPath, warPath, s) => {
        def gaeFile (path :String*) = gaeSdkPath.map(_ +: path mkString(File.separator))
        val module = gwtModule.getOrElse(gwtModules.headOption.getOrElse{s.log.error("Found no .gwt.xml files."); ""})
        val cp = reorderClasspath(dependencyClasspath) ++ getDepSources(thisProject.dependencies, pstate) ++
          gaeFile("lib", "appengine-tools-api.jar").toList :+ javaSource.absolutePath
        val javaArgs = javaOpts ++ (gaeFile("lib", "agent", "appengine-agent.jar") match {
          case None => Nil
          case Some(path) => List("-javaagent:" + path)
        })
        val gwtArgs = gaeSdkPath match {
          case None => Nil
          case Some(path) => List(
            "-server", "com.google.appengine.tools.development.gwt.AppEngineLauncher")
        }
        val command = mkGwtCommand(
          cp, javaArgs, "com.google.gwt.dev.DevMode", warPath, gwtArgs, module)
        s.log.info("Running GWT devmode on: " + module)
        s.log.debug("Running GWT devmode command: " + command)
        command.!
      }
    },

    gwtSuperDevMode := {
//    <<= (baseDirectory, dependencyClasspath in Gwt, thisProject in Gwt,  state in Gwt, javaSource in Compile, javaOptions in Gwt,
//      gwtModules, gaeSdkPath, gwtWebappPath, streams, gwtDevTemporaryPath, gwtBindAddress) map {
//      (bd, dependencyClasspath, thisProject, pstate, javaSource, javaOpts,  gwtModules, gaeSdkPath, warPath, s, gwtTmp, bindAddress) => {

        val bd = baseDirectory.value
        val depClasspath = (dependencyClasspath in Gwt).value
        val pstate = (state in Gwt).value
        val s = streams.value
        val project = thisProject.value
        val jSource = (javaSource in Compile).value

        val srcDirs =
          for(d <- (Seq(jSource.absolutePath) ++ getDepSources(project.dependencies, pstate)) map(new File(_)) if d.isDirectory)
          yield d.getAbsolutePath
        s.log.info("src dirs:" + srcDirs.mkString(", "))
        def gaeFile (path :String*) = gaeSdkPath.value.map(_ +: path mkString(File.separator))
        val module = gwtModule.getOrElse(gwtModules.value.headOption.getOrElse{s.log.error("Found no .gwt.xml files."); "nomodule"})
        val srcs = getDepSources(project.dependencies, pstate)

        val cp = reorderClasspath(depClasspath) ++ srcs ++
          gaeFile("lib", "appengine-tools-api.jar").toList :+ jSource.absolutePath
        val javaArgs = (javaOptions in Gwt).value ++ (gaeFile("lib", "agent", "appengine-agent.jar") match {
          case None => Nil
          case Some(path) => List("-javaagent:" + path)
        })

        def mkGwtSuperDevCmd = {
          val b = Seq.newBuilder[String]
          b += "java"
          b ++= Seq("-cp", cp.mkString(File.pathSeparator))
          b ++= javaArgs
          b += "com.google.gwt.dev.codeserver.CodeServer"
          (gwtBindAddress.value) map (b ++= Seq("-bindAddress", _))
          b ++= srcDirs.flatMap(Seq("-src", _))
          b ++= Seq("-workDir", gwtDevTemporaryPath.value.getAbsolutePath)
          b += module
          b.result.mkString(" ")
        }

        val command = mkGwtSuperDevCmd
        s.log.debug("gwt superdev cmd:\n" + command)
        s.log.info("Start GWT super dev mode")
        command.!
    },

    gwtCompile <<= ((classDirectory in Compile,
      dependencyClasspath in Gwt,
      thisProject in Gwt,
      state in Gwt,
      javaSource in Compile,
      javaOptions in Gwt,
      gwtModules,
      gwtTemporaryPath,
      streams,
      gwtForceCompile) map {
      (classDirectory, dependencyClasspath, thisProject, pstate, javaSource, javaOpts, gwtModules, warPath, s, force) => {

        val srcDirs = Seq(javaSource.absolutePath) ++ getDepSources(thisProject.dependencies, pstate)

        val cp = Seq(classDirectory.absolutePath) ++ reorderClasspath(dependencyClasspath) ++ srcDirs

        s.log.info("GWT output path: " + warPath.absolutePath)

        val needToCompile : Boolean = {
          s.log.info("Checking GWT module updates: " + gwtModules.mkString(", "))
          val gwtFiles : Seq[File] = (warPath ** "*.nocache.js").get
          if(gwtFiles.isEmpty) {
            s.log.info("No GWT output is found in " + warPath)
            true
          }
          else {
            val lastCompiled = gwtFiles.map(_.lastModified).max
            val moduleDirs = for(d <- srcDirs; m <- (new File(d) ** "*.gwt.xml").get) yield { m.getParentFile }
            val gwtSrcs = for(m <- moduleDirs; f <- (m ** "*").get) yield f
            gwtSrcs.find(lastCompiled < _.lastModified).isDefined
          }
        }
        if(force || needToCompile) {
          IO.createDirectory(new File(warPath.absolutePath))
          val command = mkGwtCommand(
            cp, javaOpts, "com.google.gwt.dev.Compiler", warPath, Nil, gwtModules.mkString(" "))
          s.log.info("Compiling GWT modules: " + gwtModules.mkString(","))
          s.log.debug("Running GWT compiler command: " + command)
          command.!
        }
        else {
          s.log.info("GWT modules are up to date")
          0
        }
      }
    }).dependsOn(compile in Compile, copyResources in Compile),
    unmanagedResourceDirectories in Compile <+= (javaSource in Compile) { (js:File) => js },
    excludeFilter in Compile in unmanagedResources := "*.java",
    webappResources in Compile <+= (gwtTemporaryPath) { (t: File) => t },
    packageBin in Compile <<= (packageBin in Compile).dependsOn(gwtCompile),
    packageWar in Compile <<= (packageWar in Compile).dependsOn(gwtCompile)

    //commands ++= Seq(gwtSetModule)
  )


  def reorderClasspath(cp:Id[Keys.Classpath]) = {
    val (gwtLibs, others) = cp.map(_.data.absolutePath).partition(_.contains("gwt-"))
    gwtLibs ++ others
  }

  
  def getDepSources(deps : Seq[ClasspathDep[ProjectRef]], state : State) : Set[String] = {
    var sources = Set.empty[String]
    val structure = Project.extract(state).structure
    def get[A] = setting[A](structure)_
    deps.foreach{
    dep=>
      sources +=  (get(dep.project, Keys.sourceDirectory, Compile).get.toString + "/java")
      sources ++= getDepSources(Project.getProject(dep.project, structure).get.dependencies, state)
    }
    sources
  }

  def setting[T](structure: BuildStructure)(ref: ProjectRef, key: SettingKey[T], configuration: Configuration): Option[T] = key in (ref, configuration) get structure.data

  private def mkGwtCommand(cp: Seq[String], javaArgs: Seq[String], clazz: String, warPath: File,
                           gwtArgs: Seq[String], modules: String) =
    (List("java", "-cp", cp.mkString(File.pathSeparator)) ++ javaArgs ++
     List(clazz, "-war", warPath.absolutePath) ++ gwtArgs :+ modules).mkString(" ")



  private def findGwtModules(srcRoot: File): Seq[String] = {
    import Path.relativeTo
    val files = (srcRoot ** "*.gwt.xml").get
    val relativeStrings = files.flatMap(_ x relativeTo(srcRoot)).map(_._2)
    relativeStrings.map(_.dropRight(".gwt.xml".length).replace(File.separator, "."))
  }



}



object Version {

  case class VersionInfo(major:Int, minor:Int, remaining:String)

  def unapply(s:String) : Option[VersionInfo] = {
    try {
      val c = s.split("\\.")
      if(c.length >= 2)
        Some(VersionInfo(c(0).toInt, c(1).toInt, c.takeRight(2).mkString(".")))
      else
        None
    }
    catch {
      case e:NumberFormatException => None
    }
  }
}
