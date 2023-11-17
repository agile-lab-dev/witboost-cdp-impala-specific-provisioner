package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.commons.client.cdp.dw.CdpDwClient
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClient
import cats.implicits._

class CdpValidator(cdpEnvClient: CdpEnvClient, cdpDwClient: CdpDwClient) {

  def cdpEnvironmentExists(cdpEnvironment: String): Boolean =
    cdpEnvClient.describeEnvironment(cdpEnvironment).isRight

  def cdwVirtualClusterExists(
      cdpEnvironment: String,
      virtualWarehouseName: String
  ): Boolean = {
    val result = for {
      environment <- cdpEnvClient.describeEnvironment(cdpEnvironment).leftMap(_ => false)
      clusterOption <- cdpDwClient
        .findClusterByEnvironmentCrn(environment.getCrn)
        .leftMap(_ => false)
      cluster <- clusterOption.toRight[Boolean](false)
      virtualWarehouseOption <- cdpDwClient
        .findVwByName(cluster.getId, virtualWarehouseName)
        .leftMap(_ => false)
      _ <- virtualWarehouseOption.toRight[Boolean](false)
    } yield true
    result.merge
  }

}
