package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.mesh.repository.Repository
import it.agilelab.provisioning.mesh.self.service.lambda.core.model.Role

class RoleValidator(roleRepository: Repository[Role, String, Unit]) {

  def rolesExist(roles: Seq[String]): Boolean =
    roles.forall(roleExists)

  private def roleExists(role: String): Boolean =
    roleRepository.findById(role) match {
      case Right(Some(_)) => true
      case _              => false
    }

}
