import DependenciesVersions._
import sbt._

object Dependencies {
  val akkaDependencies = Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion
  )

  val additionalResolvers = Seq(
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
  )
}
