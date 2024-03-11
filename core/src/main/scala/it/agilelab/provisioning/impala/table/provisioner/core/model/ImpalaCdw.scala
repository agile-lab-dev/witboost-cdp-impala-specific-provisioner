package it.agilelab.provisioning.impala.table.provisioner.core.model

import cats.Show
import cats.implicits.{ showInterpolator, toBifunctorOps }
import cats.syntax.functor._
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{ Decoder, Encoder, Json }
import it.agilelab.provisioning.impala.table.provisioner.core.model.ComponentDecodeError.DecodeErr
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{ OutputPort, StorageArea }
import it.agilelab.provisioning.mesh.self.service.api.model.{ Component, ProvisionRequest }
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column

sealed trait ImpalaCdw {
  def databaseName: String
}

sealed trait ImpalaTableCdw extends ImpalaCdw {
  def tableName: String
  def format: ImpalaFormat
  def location: String
  def partitions: Option[Seq[String]]
  def tableParams: Option[TableParams]
}

sealed trait ImpalaViewCdw extends ImpalaCdw {
  def viewName: String
}

sealed trait ComponentDecodeError

object ComponentDecodeError {

  /** Decode error
    * @param error: a [[String]] error explanation
    */
  final case class DecodeErr(error: String) extends ComponentDecodeError

  implicit val showComponentDecodeError: Show[ComponentDecodeError] = Show.show {
    case e: DecodeErr => show"DecodeErr(${e.error})"
  }

}

object ImpalaCdw {
  implicit val encodeImpalaCdw: Encoder[ImpalaCdw] = Encoder.instance {
    case publicCdw @ PublicImpalaTableCdw(_, _, _, _, _, _, _, _)          => publicCdw.asJson
    case storageAreaCdw @ PrivateImpalaStorageAreaCdw(_, _, _, _, _, _, _) => storageAreaCdw.asJson
    case privateCdw @ PrivateImpalaTableCdw(_, _, _, _, _, _)              => privateCdw.asJson
    case privateViewCdw @ PrivateImpalaViewCdw(_, _, _)                    => privateViewCdw.asJson
    case privateSAViewCdw @ PrivateImpalaStorageAreaViewCdw(_, _, _, _)    => privateSAViewCdw.asJson

  }

  implicit val decodeImpalaCdw: Decoder[ImpalaCdw] =
    List[Decoder[ImpalaCdw]](
      Decoder[PublicImpalaTableCdw].widen,
      Decoder[PrivateImpalaStorageAreaCdw].widen,
      Decoder[PrivateImpalaTableCdw].widen,
      Decoder[PrivateImpalaViewCdw].widen,
      Decoder[PrivateImpalaStorageAreaViewCdw].widen
    ).reduceLeft(_ or _)

  implicit class ImpalaProvisionRequestOps(provisionRequest: ProvisionRequest[Json, Json]) {
    def getOutputPortRequest[COMP_SPEC](implicit
        specDecoder: Decoder[COMP_SPEC]
    ): Either[DecodeErr, OutputPort[COMP_SPEC]] =
      provisionRequest.component
        .toRight(DecodeErr("Received provisioning request does not contain a component"))
        .flatMap {
          case c: OutputPort[_] =>
            c.specific
              .as[COMP_SPEC]
              .map(specific => c.copy(specific = specific))
              .leftMap(e => DecodeErr(show"$e"))
          case _ =>
            Left(DecodeErr("The provided component is not accepted by this provisioner"))
        }

    def getStorageAreaRequest[COMP_SPEC](implicit
        specDecoder: Decoder[COMP_SPEC]
    ): Either[DecodeErr, StorageArea[COMP_SPEC]] =
      provisionRequest.component
        .toRight(DecodeErr("Received provisioning request does not contain a component"))
        .flatMap {
          case c: StorageArea[_] =>
            c.specific
              .as[COMP_SPEC]
              .map(specific => c.copy(specific = specific))
              .leftMap(e => DecodeErr(show"$e"))
          case _ =>
            Left(DecodeErr("The provided component is not accepted by this provisioner"))
        }
  }
}

final case class TableParams(
    header: Option[Boolean],
    delimiter: Option[String],
    tblProperties: Map[String, String]
)

final case class PublicImpalaTableCdw(
    override val databaseName: String,
    override val tableName: String,
    cdpEnvironment: String,
    cdwVirtualWarehouse: String,
    override val format: ImpalaFormat,
    override val location: String,
    override val partitions: Option[Seq[String]],
    override val tableParams: Option[TableParams]
) extends ImpalaTableCdw

final case class PrivateImpalaTableCdw(
    override val databaseName: String,
    override val tableName: String,
    override val format: ImpalaFormat,
    override val location: String,
    override val partitions: Option[Seq[String]],
    override val tableParams: Option[TableParams]
) extends ImpalaTableCdw

final case class PrivateImpalaStorageAreaCdw(
    override val databaseName: String,
    override val tableName: String,
    override val format: ImpalaFormat,
    override val location: String,
    override val partitions: Option[Seq[String]],
    override val tableParams: Option[TableParams],
    tableSchema: Seq[Column]
) extends ImpalaTableCdw

final case class PrivateImpalaViewCdw(
    override val databaseName: String,
    tableName: String,
    override val viewName: String
) extends ImpalaViewCdw

final case class PrivateImpalaStorageAreaViewCdw(
    override val databaseName: String,
    override val viewName: String,
    queryStatement: String,
    tableSchema: Option[Seq[Column]] = None
) extends ImpalaViewCdw
