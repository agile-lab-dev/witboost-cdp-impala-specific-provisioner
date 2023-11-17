package it.agilelab.provisioning.impala.table.provisioner.repository

import cats.Show
import cats.implicits.toTraverseOps
import com.typesafe.config.{ Config, ConfigFactory, ConfigObject, ConfigValue }
import it.agilelab.provisioning.impala.table.provisioner.repository.RoleConfigRepository._
import it.agilelab.provisioning.mesh.repository.{ Repository, RepositoryError }
import it.agilelab.provisioning.mesh.self.service.lambda.core.model.Role

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

sealed trait RoleConfigRepositoryError extends Exception with Product with Serializable

object RoleConfigRepositoryError {
  final case class ConfKeyNotFoundErr(key: String) extends RoleConfigRepositoryError
  final case class ConfMismatchShapeErr(config: ConfigValue) extends RoleConfigRepositoryError
  final case class ConfUnsupportedOpErr() extends RoleConfigRepositoryError

  implicit val showConfError: Show[RoleConfigRepositoryError] = Show.show {
    case e: ConfKeyNotFoundErr =>
      s"ConfKeyNotFoundErr(${e.key})"
    case e: ConfMismatchShapeErr =>
      s"ConfMismatchShapeErr(${e.config.render()})"
    case e: ConfUnsupportedOpErr =>
      s"ConfUnsupportedOpErr: Current repository allows only query operations, not upsert"
  }
}

class RoleConfigRepository extends Repository[Role, String, Unit] {

  lazy val provisionerConfig: Config = ConfigFactory.load().getConfig(ROLE_CONFIG)

  override def findById(id: String): Either[RepositoryError, Option[Role]] = Try {
    provisionerConfig.getConfig(id)
  }.toOption
    .traverse(createRole)
    .left
    .map(confError => RepositoryError.FindEntityByIdErr(id, confError))

  override def findAll(filter: Option[Unit]): Either[RepositoryError, Seq[Role]] =
    provisionerConfig.root().entrySet().asScala.toList.traverse { entry =>
      entry.getValue match {
        case configObject: ConfigObject =>
          createRole(configObject.toConfig).left.map(confError =>
            RepositoryError.FindAllEntitiesErr(filter, confError))
        case _ =>
          Left(
            RepositoryError.FindAllEntitiesErr(
              filter,
              RoleConfigRepositoryError.ConfMismatchShapeErr(entry.getValue))
          )
      }
    }

  override def create(entity: Role): Either[RepositoryError, Unit] = Left(
    RepositoryError
      .CreateEntityFailureErr(entity, RoleConfigRepositoryError.ConfUnsupportedOpErr()))
  override def delete(id: String): Either[RepositoryError, Unit] = Left(
    RepositoryError.DeleteEntityErr(id, RoleConfigRepositoryError.ConfUnsupportedOpErr()))
  override def update(entity: Role): Either[RepositoryError, Unit] = Left(
    RepositoryError
      .UpdateEntityFailureErr(entity, RoleConfigRepositoryError.ConfUnsupportedOpErr()))

  /** Creates a role from a specific reference of a config. This config must already be pointing to the object whose shape is the expected role
    * @param config Config object that must point to the field matching the Role object
    */
  private def createRole(config: Config): Either[RoleConfigRepositoryError, Role] = for {
    name <- Try(config.getString(ROLE_NAME)).toEither.left.map(_ =>
      RoleConfigRepositoryError.ConfKeyNotFoundErr(ROLE_NAME))
    cdpRole <- Try(config.getString(CDP_ROLE)).toEither.left.map(_ =>
      RoleConfigRepositoryError.ConfKeyNotFoundErr(CDP_ROLE))
  } yield Role(
    name = name,
    domain = "",
    iamRole = "",
    iamRoleArn = "",
    cdpRole = cdpRole,
    cdpRoleCrn = "")

}

object RoleConfigRepository {
  val ROLE_CONFIG: String = "roles"
  val ROLE_NAME: String = "name"
  val CDP_ROLE: String = "cdpRole"
}
