package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

import cats.Show
import cats.implicits.toBifunctorOps
import com.typesafe.config.Config
import it.agilelab.provisioning.commons.config.ConfError
import it.agilelab.provisioning.commons.config.ConfError.{ ConfDecodeErr, ConfKeyNotFoundErr }
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration

import scala.util.Try

sealed trait ConnectionConfig {
  def host: String
  def port: String
  def schema: String
  def user: String
  def password: String

  def setCredentials(user: String, password: String): ConnectionConfig
}
final case class UsernamePasswordConnectionConfig(
    override val host: String,
    override val port: String,
    override val schema: String,
    override val user: String,
    override val password: String,
    useSSL: Boolean
) extends ConnectionConfig {
  override def setCredentials(user: String, password: String): ConnectionConfig =
    copy(user = user, password = password)

}

final case class KerberosConnectionConfig(
    override val host: String,
    override val port: String,
    override val schema: String,
    krbRealm: Option[String],
    krbHostFQDN: String,
    krbServiceName: String,
    useSSL: Boolean
) extends ConnectionConfig {
  override val user: String = ""
  override val password: String = ""
  override def setCredentials(user: String, password: String): ConnectionConfig = this
}

object ConnectionConfig {

  /** Initializes a ConnectionConfig based on a given configuration, allowing to override fields by passing them as parameters
    * @param config Config in the shape { port: int, schema: int }
    * @param host Connection host
    * @param port Optional Connection port, if not given it will be retrieved from config
    * @param schema Optional Schema, if not given it will be retrieve from config
    * @return Left([[ConfError]]) when a not given connection configuration is not found on the config object,
    *         Right([[ConnectionConfig]]) with the connection string parameters if retrieved correctly
    */
  def getFromConfig(
      config: Config,
      host: String,
      port: Option[Int] = None,
      schema: Option[String] = None
  ): Either[ConfError, ConnectionConfig] = for {
    authType <-
      Try(config.getString(ApplicationConfiguration.JDBC_AUTH_TYPE)).toEither.leftMap(_ =>
        ConfKeyNotFoundErr(ApplicationConfiguration.JDBC_AUTH_TYPE))
    impalaPort <- port.fold(
      Try(config.getInt(ApplicationConfiguration.JDBC_PORT)).toEither.leftMap(_ =>
        ConfKeyNotFoundErr(ApplicationConfiguration.JDBC_PORT)))(Right(_))
    impalaSchema <- schema.fold(
      Try(config.getString(ApplicationConfiguration.JDBC_SCHEMA)).toEither.leftMap(_ =>
        ConfKeyNotFoundErr(ApplicationConfiguration.JDBC_SCHEMA)))(Right(_))
    ssl <-
      Try(config.getBoolean(ApplicationConfiguration.JDBC_SSL)).toEither.leftMap(_ =>
        ConfKeyNotFoundErr(ApplicationConfiguration.JDBC_SCHEMA))
    cc <- authType match {
      case ApplicationConfiguration.JDBC_SIMPLE_AUTH =>
        Right(
          UsernamePasswordConnectionConfig(host, impalaPort.toString, impalaSchema, "", "", ssl))
      case ApplicationConfiguration.JDBC_KERBEROS_AUTH =>
        getKerberosFromConfig(config, host, impalaPort, impalaSchema, ssl)
      case authType =>
        Left(ConfDecodeErr(s"Received JDBC authentication type '$authType' which is not supported"))
    }
  } yield cc

  private def getKerberosFromConfig(
      config: Config,
      host: String,
      port: Int,
      schema: String,
      ssl: Boolean
  ): Either[ConfError, ConnectionConfig] = for {
    krbRealm <- Right(Try(config.getString(ApplicationConfiguration.JDBC_KRBREALM)).toOption)
    krbHostFQDN <- Try(config.getString(ApplicationConfiguration.JDBC_KRBHOSTFQDN)).toEither.left
      .flatMap(_ => Right(host))
    krbServiceName <- Try(config.getString(ApplicationConfiguration.JDBC_KRBSERVICENAME)).toEither
      .leftMap(_ => ConfKeyNotFoundErr(ApplicationConfiguration.JDBC_KRBSERVICENAME))
  } yield KerberosConnectionConfig(
    host,
    port.toString,
    schema,
    krbRealm,
    krbHostFQDN,
    krbServiceName,
    ssl)

  implicit def showConnectionConfig: Show[ConnectionConfig] = Show.show {
    case KerberosConnectionConfig(host, port, schema, krbRealm, krbHostFQDN, krbServiceName, ssl) =>
      s"KerberosConnectionConfig($host, $port, $schema, $krbRealm, $krbHostFQDN, $krbServiceName, useSSL=$ssl)"
    case UsernamePasswordConnectionConfig(host, port, schema, user, password, ssl) =>
      s"UsernamePasswordConnectionConfig($host, $port, $schema, $user, $password, useSSL=$ssl)"
  }
}
