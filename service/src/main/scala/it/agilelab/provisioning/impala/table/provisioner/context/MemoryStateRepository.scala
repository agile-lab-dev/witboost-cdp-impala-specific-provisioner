package it.agilelab.provisioning.impala.table.provisioner.context

import it.agilelab.provisioning.mesh.repository.{ Repository, RepositoryError }
import it.agilelab.provisioning.mesh.self.service.api.model.ApiResponse

import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.Map
import scala.jdk.CollectionConverters._

class MemoryStateRepository extends Repository[ApiResponse.ProvisioningStatus, String, Unit] {

  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private val status: Map[String, ApiResponse.ProvisioningStatus] =
    new ConcurrentHashMap[String, ApiResponse.ProvisioningStatus]().asScala

  override def findById(
      id: String
  ): Either[RepositoryError, Option[ApiResponse.ProvisioningStatus]] = Right(status.get(id))

  override def findAll(
      filter: Option[Unit]
  ): Either[RepositoryError, Seq[ApiResponse.ProvisioningStatus]] = Right(status.values.toList)

  override def create(entity: ApiResponse.ProvisioningStatus): Either[RepositoryError, Unit] =
    Right(status.put(entity.id, entity))

  override def delete(id: String): Either[RepositoryError, Unit] = Right(status.remove(id))

  override def update(entity: ApiResponse.ProvisioningStatus): Either[RepositoryError, Unit] =
    Right(status.update(entity.id, entity))

}
