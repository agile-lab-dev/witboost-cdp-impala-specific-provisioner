package it.agilelab.provisioning.impala.table.provisioner.common

import io.circe.Json
import it.agilelab.provisioning.mesh.self.service.api.model.{
  Component,
  DataProduct,
  ProvisionRequest
}
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{
  DataContract,
  OutputPort,
  StorageArea
}
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }

object ProvisionRequestFaker {

  def apply[SPECIFIC, COMP_SPECIFIC](
      specific: SPECIFIC
  ): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    ProvisionRequestFakerBuilder(
      id = "urn:dmb:dp:domain:dp-name:0",
      name = "dp-name",
      domain = "domain:domain",
      environment = "poc",
      version = "0.0.1",
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      ownerGroup = "ownerGroup",
      specific = specific,
      components = Seq.empty,
      component = None
    )
}

case class ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC](
    id: String,
    name: String,
    domain: String,
    environment: String,
    version: String,
    dataProductOwner: String,
    devGroup: String,
    ownerGroup: String,
    specific: SPECIFIC,
    components: Seq[Json],
    component: Option[Component[COMP_SPECIFIC]]
) {
  def withId(id: String): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(id = id)
  def withName(name: String): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(name = name)
  def withDomain(domain: String): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(domain = domain)
  def withEnvironment(environment: String): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(environment = environment)
  def withVersion(version: String): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(version = version)
  def withDataProductOwner(
      dataProductOwner: String
  ): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(dataProductOwner = dataProductOwner)
  def withDevGroup(devGroup: String): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(devGroup = devGroup)
  def withOwnerGroup(ownerGroup: String): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(ownerGroup = ownerGroup)
  def withSpecific(specific: SPECIFIC): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(specific = specific)
  def withComponents(components: Seq[Json]): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(components = components)
  def withComponent(
      component: Component[COMP_SPECIFIC]
  ): ProvisionRequestFakerBuilder[SPECIFIC, COMP_SPECIFIC] =
    this.copy(component = Some(component))

  def build(): ProvisionRequest[SPECIFIC, COMP_SPECIFIC] =
    ProvisionRequest(
      dataProduct = DataProduct[SPECIFIC](
        id = id,
        name = name,
        domain = domain,
        environment = environment,
        version = version,
        dataProductOwner = dataProductOwner,
        devGroup = devGroup,
        ownerGroup = ownerGroup,
        specific = specific,
        components = components
      ),
      component = component
    )

}

object OutputPortFaker {
  def apply[SPECIFIC](specific: SPECIFIC): OutputPortFakerBuilder[SPECIFIC] =
    OutputPortFakerBuilder(
      id = "urn:dmb:cmp:domain:dp-name:0:cmp-name",
      name = "cmp-name",
      description = "description",
      version = "0.0.1",
      dataContract = DataContract(
        schema = Seq(
          Column(
            "id",
            ColumnDataType.INT,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None)
        )
      ),
      specific = specific
    )
}

final case class OutputPortFakerBuilder[SPECIFIC](
    id: String,
    name: String,
    description: String,
    version: String,
    dataContract: DataContract,
    specific: SPECIFIC
) {
  def withId(id: String): OutputPortFakerBuilder[SPECIFIC] =
    this.copy(id = id)
  def withName(name: String): OutputPortFakerBuilder[SPECIFIC] =
    this.copy(name = name)
  def withDescription(description: String): OutputPortFakerBuilder[SPECIFIC] =
    this.copy(description = description)
  def withVersion(version: String): OutputPortFakerBuilder[SPECIFIC] =
    this.copy(version = version)
  def withDataContract(dataContract: DataContract): OutputPortFakerBuilder[SPECIFIC] =
    this.copy(dataContract = dataContract)
  def withSpecific(specific: SPECIFIC): OutputPortFakerBuilder[SPECIFIC] =
    this.copy(specific = specific)

  def build(): OutputPort[SPECIFIC] = OutputPort[SPECIFIC](
    id = id,
    name = name,
    description = description,
    version = version,
    dataContract = dataContract,
    specific = specific
  )
}

object StorageAreaFaker {
  def apply[SPECIFIC](specific: SPECIFIC): StorageAreaFakerBuilder[SPECIFIC] =
    StorageAreaFakerBuilder(
      id = "urn:dmb:cmp:domain:dp-name:0:cmp-name",
      name = "cmp-name",
      description = "description",
      owners = Seq.empty,
      specific = specific
    )
}

final case class StorageAreaFakerBuilder[SPECIFIC](
    id: String,
    name: String,
    description: String,
    owners: Seq[String],
    specific: SPECIFIC
) {
  def withId(id: String): StorageAreaFakerBuilder[SPECIFIC] =
    this.copy(id = id)
  def withName(name: String): StorageAreaFakerBuilder[SPECIFIC] =
    this.copy(name = name)
  def withDescription(description: String): StorageAreaFakerBuilder[SPECIFIC] =
    this.copy(description = description)
  def withSpecific(specific: SPECIFIC): StorageAreaFakerBuilder[SPECIFIC] =
    this.copy(specific = specific)
  def withOwners(owners: Seq[String]): StorageAreaFakerBuilder[SPECIFIC] =
    this.copy(owners = owners)

  def build(): StorageArea[SPECIFIC] = StorageArea[SPECIFIC](
    id = id,
    name = name,
    description = description,
    owners = owners,
    specific = specific
  )
}
