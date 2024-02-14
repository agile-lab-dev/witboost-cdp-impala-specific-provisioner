package it.agilelab.provisioning.impala.table.provisioner.context

import com.typesafe.config.{ Config, ConfigFactory, ConfigRenderOptions }

/** This is a wrapper for the Typesafe [[Config]] class. A Specific Provisioner app
  * can extend this trait to obtain the main configuration values.
  */
trait ApplicationConfiguration extends ConfigurationModel {

  lazy val impalaConfig: Config = ConfigFactory.load().getConfig(IMPALA)
  lazy val provisionerConfig: Config = ConfigFactory.load().getConfig(PROVISIONER)
  lazy val hdfsConfig: Config = ConfigFactory.load().getConfig(HDFS)
  lazy val rangerConfig: Config = ConfigFactory.load().getConfig(RANGER)
  lazy val principalsMapperConfig: Config = ConfigFactory.load().getConfig(PRINCIPAL_MAPPING_PLUGIN)

  private val formatter = ConfigRenderOptions.concise().setFormatted(true)

  /** Create a human readable version of the given [[Config]] object formatted as JSON
    *
    * @param c the [[Config]] to be beautified
    * @return a [[String]] representing the given [[Config]] object formatted as JSON
    */
  def printBeautifiedConfigJSON(c: Config = impalaConfig): String = c.root().render(formatter)

}

object ApplicationConfiguration extends ApplicationConfiguration
