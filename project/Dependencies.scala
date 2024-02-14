import sbt._

trait Dependencies {
  lazy val scalaCommonsVrs = "0.0.0-SNAPSHOT-15dfac13ba.wit-1430-scala-mesh"
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
  lazy val scalaMeshPrincipalsMappingSamples =
    "it.agilelab.provisioning" %% "scala-mesh-principals-mapping-samples" % scalaCommonsVrs

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

  private val ldaptiveVrs = "2.3.0"
  lazy val ldaptiveDependencies = "org.ldaptive" % "ldaptive" % ldaptiveVrs

  private val pureconfigVrs = "0.17.5"
  lazy val pureconfigDependencies = "com.github.pureconfig" %% "pureconfig" % pureconfigVrs

  lazy val logbackVersion = "1.4.5"
  lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % logbackVersion
  lazy val logbackCore = "ch.qos.logback" % "logback-core" % logbackVersion
  lazy val logbackDependencies: Seq[ModuleID] = Seq(logbackClassic, logbackCore)

  lazy val serviceDependencies: Seq[ModuleID] = Seq(
    scalaMeshSelfLambda,
    scalaMeshRep,
    scalaCdpDl,
    scalaCdpEnv,
    ranger,
    scalaMeshPrincipalsMappingSamples,
    ldaptiveDependencies,
    pureconfigDependencies
  ) ++ logbackDependencies

  lazy val scalatestMockitoVersion = "3.2.11.0"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVrs % "test"
  lazy val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVrs % "test"
  lazy val mockito = "org.scalatestplus" %% "mockito-4-2" % scalatestMockitoVersion

  lazy val testDependencies = Seq(
    scalaTest,
    scalaMock,
    mockito
  )

}

object Dependencies extends Dependencies
