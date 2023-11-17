package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.mesh.repository.Repository
import it.agilelab.provisioning.mesh.self.service.lambda.core.model.{ DataProductEntityKey, Domain }

class DomainDataProductValidator(
    domainRepo: Repository[Domain, String, Unit],
    dpRepo: Repository[DataProductEntityKey, DataProductEntityKey, Unit]
) {

  def domainExists(domain: String): Boolean =
    domainRepo.findById(domain) match {
      case Right(Some(_)) => true
      case _              => false
    }

  def dpExists(
      domain: String,
      dp: String
  ): Boolean =
    dpRepo.findById(DataProductEntityKey(domain, dp)) match {
      case Right(Some(_)) => true
      case _              => false
    }

}
