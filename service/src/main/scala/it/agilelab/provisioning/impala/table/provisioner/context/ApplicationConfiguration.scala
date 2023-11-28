package it.agilelab.provisioning.impala.table.provisioner.context

import com.typesafe.config.{ Config, ConfigFactory, ConfigRenderOptions }

/** This is a wrapper for the Typesafe [[Config]] class. A Specific Provisioner app
  * can extend this trait to obtain the main configuration values.
  */
trait ApplicationConfiguration {

  val IMPALA = "impala"

  val DROP_ON_UNPROVISION = "drop-on-unprovision"

  /** The whole configuration starting from the "datameshProvisioner" key in the configuration files.
    */
  lazy val provisionerConfig: Config = ConfigFactory.load().getConfig(IMPALA)

  private val formatter = ConfigRenderOptions.concise().setFormatted(true)

  /** Create a human readable version of the given [[Config]] object formatted as JSON
    *
    * @param c the [[Config]] to be beautified
    * @return a [[String]] representing the given [[Config]] object formatted as JSON
    */
  def printBeautifiedConfigJSON(c: Config = provisionerConfig): String = c.root().render(formatter)

}

/** A private implementation of [[ApplicationConfiguration]] to be used only inside the framework.
  */
private[provisioner] object ApplicationConfiguration extends ApplicationConfiguration
