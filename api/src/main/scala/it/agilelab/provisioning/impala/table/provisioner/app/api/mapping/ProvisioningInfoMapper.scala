package it.agilelab.provisioning.impala.table.provisioner.app.api.mapping

import cats.Show
import cats.implicits.toShow
import io.circe.{ Decoder, Json, JsonObject }
import it.agilelab.provisioning.api.generated.definitions.Info
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  ImpalaEntityResource,
  ImpalaView
}

object ProvisioningInfoMapper {

  sealed trait InfoObject { def toJson: Json }

  final case class StringInfoObject(label: String, value: String) extends InfoObject {
    override def toJson: Json = Json.obj(
      "type"  -> Json.fromString("string"),
      "label" -> Json.fromString(label),
      "value" -> Json.fromString(value)
    )
  }

  final case class ImpalaEntityInfo(resource: ImpalaEntityResource) extends InfoObject {
    override def toJson: Json = {
      val baseFields = Seq(
        "impalaDatabase" -> StringInfoObject("database", resource.impalaEntity.database).toJson
      )
      val fields = resource.impalaEntity match {
        case ExternalTable(database, name, schema, partitions, location, format, _, _, _) =>
          baseFields ++ Seq(
            "impalaTable"    -> StringInfoObject("table", name).toJson,
            "impalaLocation" -> StringInfoObject("location", location).toJson,
            "impalaFormat"   -> StringInfoObject("format", format.show).toJson
          )
        case ImpalaView(database, name, schema, readsFromTableName) =>
          baseFields ++ Seq(
            "impalaView" -> StringInfoObject("view", name).toJson
          )
      }

      Json.fromFields(fields)
    }
  }

  def fromImpalaTableResource(resource: ImpalaEntityResource): Info =
    Info(
      publicInfo = ImpalaEntityInfo(resource).toJson,
      privateInfo = Json.obj()
    )

}
