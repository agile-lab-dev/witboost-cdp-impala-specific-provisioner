package it.agilelab.provisioning.impala.table.provisioner.context

import cats.implicits.toBifunctorOps
import com.typesafe.config.Config
import it.agilelab.provisioning.commons.config.ConfError
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
import it.agilelab.provisioning.commons.principalsmapping.{
  CdpIamPrincipals,
  PrincipalsMapper,
  PrincipalsMapperFactory
}
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.{
  ConfigurationError,
  PrincipalsMapperLoaderError
}

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }

class PrincipalsMapperPluginLoader() {

  /** Tries to loads the Principal Mapper Plugin
    * @return the requested mapper
    */
  def load(cfg: Config): Either[ContextError, PrincipalsMapper[CdpIamPrincipals]] =
    for {
      pluginClass    <- extractPluginClass(cfg).leftMap(t => ConfigurationError(t))
      mf             <- mapperFactory(pluginClass)
      specificConfig <- getSpecificConfig(mf, cfg).leftMap(t => ConfigurationError(t))
      mapper         <- mf.create(specificConfig).toEither.leftMap(t => PrincipalsMapperLoaderError(t))
    } yield mapper

  private def mapperFactory(
      pluginClass: String
  ): Either[PrincipalsMapperLoaderError, PrincipalsMapperFactory[CdpIamPrincipals]] =
    Try {
      ServiceLoader
        .load(classOf[PrincipalsMapperFactory[_]])
        .asScala
        .toList
        .filter(_.getClass.eq(Class.forName(pluginClass)))
    } match {
      case Success(mappers) =>
        mappers.headOption match {
          case Some(value: PrincipalsMapperFactory[CdpIamPrincipals]) => Right(value)
          case _ =>
            Left(
              PrincipalsMapperLoaderError(
                new Throwable(s"Couldn't load plugin with class $pluginClass")))
        }
      case Failure(exception) =>
        Left(PrincipalsMapperLoaderError(exception))
    }

  private def getSpecificConfig(
      mf: PrincipalsMapperFactory[CdpIamPrincipals],
      cfg: Config
  ): Either[ConfError, Config] = Try {
    cfg.getConfig(mf.configIdentifier)
  }.toEither.leftMap(_ => ConfKeyNotFoundErr(mf.configIdentifier))

  private def extractPluginClass(cfg: Config): Either[ConfError, String] =
    Try {
      cfg.getString(ApplicationConfiguration.PRINCIPAL_MAPPING_PLUGIN_CLASS)
    }.toEither.leftMap(_ =>
      ConfKeyNotFoundErr(ApplicationConfiguration.PRINCIPAL_MAPPING_PLUGIN_CLASS))
}
