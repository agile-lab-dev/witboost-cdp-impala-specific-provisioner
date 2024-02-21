package it.agilelab.provisioning.impala.table.provisioner.app.api.mapping

import io.circe.{ parser, Decoder }
import it.agilelab.provisioning.api.generated.definitions.ProvisioningStatus
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaTableResource
import it.agilelab.provisioning.mesh.self.service.api.model.ApiResponse
import it.agilelab.provisioning.mesh.self.service.api.model.ApiResponse.Status

object ProvisioningStatusMapper {
  def from(result: ApiResponse.ProvisioningStatus)(implicit
      decoder: Decoder[ImpalaTableResource]
  ): ProvisioningStatus = result.status match {
    case Status.COMPLETED =>
      val info = for {
        result   <- result.result
        json     <- parser.parse(result).toOption
        resource <- json.as[ImpalaTableResource].toOption
      } yield resource
      ProvisioningStatus(
        ProvisioningStatus.Status.Completed,
        "",
        info.map(ProvisioningInfoMapper.fromImpalaTableResource)
      )
    case Status.FAILED  => ProvisioningStatus(ProvisioningStatus.Status.Failed, "")
    case Status.RUNNING => ProvisioningStatus(ProvisioningStatus.Status.Failed, "")
  }
}
