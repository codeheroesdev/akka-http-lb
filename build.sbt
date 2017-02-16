name := "akka-http-lb"
version := "0.3.1"
scalaVersion := "2.11.8"

bintrayOrganization := Some("codeheroes")
bintrayPackage := "akka-http-lb"
licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

lazy val `akka-http-lb` = project.in(file("."))
  .settings(resolvers ++= Dependencies.additionalResolvers)
  .settings(libraryDependencies ++= Dependencies.akkaDependencies)
