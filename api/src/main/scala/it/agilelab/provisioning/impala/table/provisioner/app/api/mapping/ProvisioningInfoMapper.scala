package it.agilelab.provisioning.impala.table.provisioner.app.api.mapping

import cats.implicits.toShow
import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import io.circe.{ Encoder, Json }
import it.agilelab.provisioning.api.generated.definitions.Info
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  ImpalaProvisionerResource,
  ImpalaView
}
import pureconfig._
import pureconfig.generic.auto.exportReader

object ProvisioningInfoMapper {

  final case class ImpalaEntityInfo(resource: ImpalaProvisionerResource)

  object ImpalaEntityInfo {
    def impalaEntityInfoEncoder: Encoder[ImpalaEntityInfo] = (info: ImpalaEntityInfo) => {
      val baseFields = Seq(
        "impalaDatabase" -> InfoModel
          .makeStringInfoObject(
            "database",
            info.resource.impalaEntityResource.impalaEntity.database),
        "jdbcUrl" -> InfoModel.makeStringInfoObject(
          "JDBC Connection String",
          info.resource.impalaEntityResource.jdbcConnectionString)
      )
      val fields = info.resource.impalaEntityResource.impalaEntity match {
        case ExternalTable(database, name, schema, partitions, location, format, _, _, _) =>
          baseFields ++ Seq(
            "impalaTable"    -> InfoModel.makeStringInfoObject("table", name),
            "impalaLocation" -> InfoModel.makeStringInfoObject("location", location),
            "impalaFormat"   -> InfoModel.makeStringInfoObject("format", format.show)
          )
        case ImpalaView(_, name, _, _, _) =>
          baseFields ++ Seq(
            "impalaView" -> InfoModel.makeStringInfoObject("view", name)
          )
      }

      val jsonInfo = (fields ++
        ConfigSource
          .fromConfig(ApplicationConfiguration.provisionerConfig.getConfig(
            ApplicationConfiguration.PROVISION_INFO))
          .load[Map[String, InfoModel]]
          .getOrElse(Map.empty)
          .toList)
        .map { case (key, infoModel) => key -> infoModel.asJson.deepDropNullValues }

      Json.fromFields(jsonInfo)
    }
  }

  def fromImpalaTableResource(resource: ImpalaProvisionerResource): Info =
    Info(
      publicInfo = ImpalaEntityInfo(resource).asJson(ImpalaEntityInfo.impalaEntityInfoEncoder),
      privateInfo = Json.obj()
    )

}
