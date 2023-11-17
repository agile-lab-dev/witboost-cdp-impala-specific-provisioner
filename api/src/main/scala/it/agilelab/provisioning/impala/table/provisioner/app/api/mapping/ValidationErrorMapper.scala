package it.agilelab.provisioning.impala.table.provisioner.app.api.mapping

import it.agilelab.provisioning.api.generated.definitions.ValidationError
import it.agilelab.provisioning.mesh.self.service.api.model.ApiError

object ValidationErrorMapper {

  def from(result: ApiError.ValidationError): ValidationError =
    ValidationError(result.errors.toVector)
}
