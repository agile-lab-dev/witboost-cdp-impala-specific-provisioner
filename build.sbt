import Settings._

inThisBuild(
  Seq(
    organization := "it.agilelab.provisioning",
    scalaVersion := "2.13.3",
    version := ComputeVersion.version,
    resolvers += ExternalResolvers.clouderaResolver
  )
)
lazy val core = (project in file("core"))
  .settings(
    name := "impala-table-provisioner-core-commons",
    libraryDependencies ++= Dependencies.testDependencies ++ Seq(Dependencies.scalaMeshSelf),
    artifactorySettings,
    wartremoverSettings,
    k8tyGitlabPluginSettings
  )
  .enablePlugins(K8tyGitlabPlugin)

lazy val service = (project in file("service"))
  .settings(
    name := "impala-table-provisioner-service",
    unmanagedBase := baseDirectory.value / "lib",
    libraryDependencies ++= Dependencies.testDependencies ++ Seq(
      Dependencies.scalaMeshSelfLambda,
      Dependencies.scalaMeshRep,
      Dependencies.scalaCdpDl,
      Dependencies.scalaCdpEnv,
      Dependencies.ranger,
      Dependencies.scalaMeshPrincipalsMappingSamples
    ),
    artifactorySettings,
    wartremoverSettings,
    k8tyGitlabPluginSettings
  )
  .enablePlugins(K8tyGitlabPlugin)
  .dependsOn(core)

lazy val api = (project in file("api"))
  .settings(
    name := "impala-table-provisioner-api",
    libraryDependencies ++= Dependencies.testDependencies ++ Dependencies.http4sDependencies ++ Dependencies.circeDependencies
      ++ Dependencies.catsDependencies
      ++ Seq(
        Dependencies.scalaMeshSelf,
        Dependencies.scalaMeshRep,
        Dependencies.scalaCdpDw,
        Dependencies.scalaS3Gat
      ),
    artifactorySettings,
    wartremoverSettings,
    k8tyGitlabPluginSettings
  )
  .settings(
    Compile / guardrailTasks := GuardrailHelpers.createGuardrailTasks(
      (Compile / sourceDirectory).value / "openapi") { openApiFile =>
      List(
        ScalaServer(
          openApiFile.file,
          pkg = "it.agilelab.provisioning.api.generated",
          framework = "http4s",
          tracing = false
        )
      )
    },
    coverageExcludedPackages := "it.agilelab.provisioning.api.generated.*"
  )
  .enablePlugins(K8tyGitlabPlugin)
  .dependsOn(
    service % "compile->compile;test->test"
  )

lazy val root = (project in file("."))
  .settings(
    name := "impala-table-provisioner",
    mainClass in Compile := Some("it.agilelab.provisioning.impala.table.provisioner.app.Main"),
    artifactorySettings,
    dockerBuildOptions ++= Seq("--network=host"),
    dockerBaseImage := "registry.gitlab.com/agilefactory/witboost.mesh/provisioning/cdp/cicd/scala-sbt",
    dockerUpdateLatest := true,
    daemonUser := "daemon",
    Docker / version := (ThisBuild / version).value,
    Docker / packageName :=
      s"registry.gitlab.com/agilefactory/witboost.mesh/provisioning/cdp-refresh/witboost.mesh.provisioning.outputport.cdp.impala",
    Docker / dockerExposedPorts := Seq(8093)
  )
  .enablePlugins(JavaAppPackaging)
  .aggregate(core, service, api)
  .dependsOn(core, service, api)
