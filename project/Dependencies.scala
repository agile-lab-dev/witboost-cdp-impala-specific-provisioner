import sbt._

trait Dependencies {
  lazy val scalaCommonsVrs = "1.0.0"
  lazy val scalaTestVrs = "3.1.0"
  lazy val scalaMockVrs = "4.4.0"

  lazy val scalaS3Gat = "com.witboost.provisioning" %% "scala-mesh-aws-s3" % scalaCommonsVrs
  lazy val scalaCdpDl = "com.witboost.provisioning" %% "scala-mesh-cdp-dl" % scalaCommonsVrs
  lazy val scalaCdpEnv = "com.witboost.provisioning" %% "scala-mesh-cdp-env" % scalaCommonsVrs
  lazy val scalaCdpDw = "com.witboost.provisioning" %% "scala-mesh-cdp-dw" % scalaCommonsVrs
  lazy val ranger = "com.witboost.provisioning" %% "scala-mesh-ranger" % scalaCommonsVrs
  lazy val scalaMeshRep = "com.witboost.provisioning" %% "scala-mesh-repository" % scalaCommonsVrs
  lazy val scalaMeshSelf =
    "com.witboost.provisioning" %% "scala-mesh-self-service" % scalaCommonsVrs
  lazy val scalaMeshSelfLambda =
    "com.witboost.provisioning" %% "scala-mesh-self-service-lambda" % scalaCommonsVrs
  lazy val scalaMeshPrincipalsMappingSamples =
    "com.witboost.provisioning" %% "scala-mesh-principals-mapping-samples" % scalaCommonsVrs

  private val http4sVersion = "0.23.18"
  lazy val http4sDependencies: Seq[ModuleID] = Seq(
    "org.http4s" %% "http4s-ember-client",
    "org.http4s" %% "http4s-ember-server",
    "org.http4s" %% "http4s-dsl",
    "org.http4s" %% "http4s-circe"
  ).map(_ % http4sVersion)

  val catsRetryVersion = "3.1.0"
  lazy val catsDependencies: Seq[ModuleID] = Seq(
    "org.typelevel" %% "cats-effect" % "3.4.8",
    "com.github.cb372" %% "cats-retry" % catsRetryVersion)

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
