name := "akka-http-lb"

version := "1.0"

scalaVersion := "2.11.8"

lazy val `akka-http-lb` = project.in(file("."))
  .settings(resolvers ++= Dependencies.additionalResolvers)
  .settings(libraryDependencies ++= Dependencies.akkaDependencies)