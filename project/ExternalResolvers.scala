import sbt._

trait ExternalResolvers {

  val clouderaResolver = "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
}

object ExternalResolvers extends ExternalResolvers
