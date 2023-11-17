import sbt._

trait Dependencies {
  lazy val scalaCommonsVrs = "0.0.0-SNAPSHOT-94c691a.wit-365-cdp-common-l"
  lazy val scalaTestVrs = "3.1.0"
  lazy val scalaMockVrs = "4.4.0"

  lazy val scalaS3Gat = "it.agilelab.provisioning" %% "scala-mesh-aws-s3" % scalaCommonsVrs
  lazy val scalaCdpDl = "it.agilelab.provisioning" %% "scala-mesh-cdp-dl" % scalaCommonsVrs
  lazy val scalaCdpEnv = "it.agilelab.provisioning" %% "scala-mesh-cdp-env" % scalaCommonsVrs
  lazy val scalaCdpDw = "it.agilelab.provisioning" %% "scala-mesh-cdp-dw" % scalaCommonsVrs
  lazy val ranger = "it.agilelab.provisioning" %% "scala-mesh-ranger" % scalaCommonsVrs
  lazy val scalaMeshRep = "it.agilelab.provisioning" %% "scala-mesh-repository" % scalaCommonsVrs
  lazy val scalaMeshSelf = "it.agilelab.provisioning" %% "scala-mesh-self-service" % scalaCommonsVrs
  lazy val scalaMeshSelfLambda =
    "it.agilelab.provisioning" %% "scala-mesh-self-service-lambda" % scalaCommonsVrs

  private val http4sVersion = "0.23.18"
  lazy val http4sDependencies: Seq[ModuleID] = Seq(
    "org.http4s" %% "http4s-ember-client",
    "org.http4s" %% "http4s-ember-server",
    "org.http4s" %% "http4s-dsl",
    "org.http4s" %% "http4s-circe"
  ).map(_ % http4sVersion)

  lazy val catsDependencies: Seq[ModuleID] = Seq(
    "org.typelevel" %% "cats-effect" % "3.4.8"
  )

  private val circeVersion = "0.14.5"
  lazy val circeDependencies: Seq[ModuleID] = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion) ++ Seq("io.circe" %% "circe-generic-extras" % "0.14.3")

  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVrs % "test"
  lazy val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVrs % "test"

  lazy val testDependencies = Seq(
    scalaTest,
    scalaMock
  )

}

object Dependencies extends Dependencies
