import sbt._

trait ExternalResolvers {

  val clouderaResolver = "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"

  val gitlabScalaMeshCommonsResolver =
    "gitlab" at sys.env.getOrElse("GITLAB_ARTIFACT_HOST", "GITLAB_ARTIFACT_HOST")

}

object ExternalResolvers extends ExternalResolvers
