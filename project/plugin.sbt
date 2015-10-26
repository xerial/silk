addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.7.5")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.5.1")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("org.skinny-framework" % "sbt-servlet-plugin"      % "2.0.1")

scalacOptions ++= Seq("-deprecation", "-feature")
