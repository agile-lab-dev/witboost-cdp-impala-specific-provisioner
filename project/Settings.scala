import wartremover.WartRemover.autoImport._
import sbt._
import sbt.Keys._
import lmcoursier.definitions.Authentication

object Settings {
  lazy val k8tyGitlabPluginSettings = Seq(
    app.k8ty.sbt.gitlab.K8tyGitlabPlugin.gitlabProjectId := "51212612"
  )

  lazy val wartremoverSettings = Seq(
    wartremoverErrors in (Compile, compile) ++= Warts.allBut(
      Wart.Any,
      Wart.Nothing,
      Wart.Serializable,
      Wart.JavaSerializable,
      Wart.NonUnitStatements,
      Wart.ImplicitParameter,
      Wart.Throw,
      Wart.StringPlusAny,
      Wart.ToString,
      Wart.DefaultArguments,
      Wart.AsInstanceOf,
      Wart.Equals,
      Wart.Enumeration,
      Wart.TraversableOps
    ),
    wartremoverExcluded += sourceManaged.value
  )

  lazy val artifactorySettings = Seq(
    csrConfiguration ~=
      (configuration =>
        configuration.addRepositoryAuthentication(
          "gitlab",
          Authentication(
            sys.env.getOrElse("GITLAB_ARTIFACT_USER", "GITLAB_ARTIFACT_USER"),
            sys.env.getOrElse("GITLAB_ARTIFACT_TOKEN", "GITLAB_ARTIFACT_TOKEN")
          )
        )),
    updateClassifiers / csrConfiguration := csrConfiguration.value,
    updateSbtClassifiers / csrConfiguration := csrConfiguration.value,
    resolvers ++= Seq(
      ExternalResolvers.gitlabScalaMeshCommonsResolver,
      ExternalResolvers.clouderaResolver
    )
  )

}
