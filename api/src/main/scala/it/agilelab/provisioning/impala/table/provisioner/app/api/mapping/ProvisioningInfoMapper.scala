package it.agilelab.provisioning.impala.table.provisioner.app.api.mapping

import cats.Show
import cats.implicits.toShow
import io.circe.{ Decoder, Json, JsonObject }
import it.agilelab.provisioning.api.generated.definitions.Info
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaTableOutputPortResource

object ProvisioningInfoMapper {

  sealed trait InfoObject { def toJson: Json }

  final case class StringInfoObject(label: String, value: String) extends InfoObject {
    override def toJson: Json = Json.obj(
      "type"  -> Json.fromString("string"),
      "label" -> Json.fromString(label),
      "value" -> Json.fromString(value)
    )
  }

  final case class ImpalaTableOutputPortInfo(resource: ImpalaTableOutputPortResource)
      extends InfoObject {
    override def toJson: Json = Json.obj(
      "impalaTable"    -> StringInfoObject("table", resource.table.tableName).toJson,
      "impalaDatabase" -> StringInfoObject("database", resource.table.database).toJson,
      "impalaLocation" -> StringInfoObject("location", resource.table.location).toJson,
      "impalaFormat"   -> StringInfoObject("format", resource.table.format.show).toJson
    )
  }

  def fromImpalaTableOutputPortResource(resource: ImpalaTableOutputPortResource): Info =
    Info(
      publicInfo = ImpalaTableOutputPortInfo(resource).toJson,
      privateInfo = Json.obj()
    )

}
