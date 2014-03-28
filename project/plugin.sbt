
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.2")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.7.1")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.5.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.3.8")

addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "0.4.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

libraryDependencies <+= sbtVersion("org.scala-sbt" % "scripted-plugin" % _)

scalacOptions ++= Seq("-deprecation", "-feature")
