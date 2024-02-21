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
  def tableName: String
  def format: ImpalaFormat
  def location: String
  def partitions: Option[Seq[String]]
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
    case publicCdw @ PublicImpalaCdw(_, _, _, _, _, _, _)        => publicCdw.asJson
    case storageAreaCdw @ ImpalaStorageAreaCdw(_, _, _, _, _, _) => storageAreaCdw.asJson
    case privateCdw @ PrivateImpalaCdw(_, _, _, _, _)            => privateCdw.asJson
  }

  implicit val decodeImpalaCdw: Decoder[ImpalaCdw] =
    List[Decoder[ImpalaCdw]](
      Decoder[PublicImpalaCdw].widen,
      Decoder[ImpalaStorageAreaCdw].widen,
      Decoder[PrivateImpalaCdw].widen)
      .reduceLeft(_ or _)

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

final case class PublicImpalaCdw(
    override val databaseName: String,
    override val tableName: String,
    cdpEnvironment: String,
    cdwVirtualWarehouse: String,
    override val format: ImpalaFormat,
    override val location: String,
    override val partitions: Option[Seq[String]]
) extends ImpalaCdw

final case class PrivateImpalaCdw(
    override val databaseName: String,
    override val tableName: String,
    override val format: ImpalaFormat,
    override val location: String,
    override val partitions: Option[Seq[String]]
) extends ImpalaCdw

final case class ImpalaStorageAreaCdw(
    override val databaseName: String,
    override val tableName: String,
    override val format: ImpalaFormat,
    override val location: String,
    override val partitions: Option[Seq[String]],
    tableSchema: Seq[Column]
) extends ImpalaCdw
