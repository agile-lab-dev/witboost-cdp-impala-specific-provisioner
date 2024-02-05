package it.agilelab.provisioning.impala.table.provisioner.context

trait ConfigurationModel {
  val PROVISIONER: String = "provisioner"
  val PROVISION_CLOUD_TYPE: String = "provision-cloud"
  val PROVISION_CLUSTER_NAME: String = "cluster-name"

  private val networking: String = "networking"
  private val httpServer: String = "httpServer"
  private val interface: String = "interface"
  private val port: String = "port"

  val NETWORKING_HTTPSERVER_INTERFACE: String = s"$networking.$httpServer.$interface"
  val NETWORKING_HTTPSERVER_PORT: String = s"$networking.$httpServer.$port"

  val IMPALA: String = "impala"

  val DROP_ON_UNPROVISION: String = "drop-on-unprovision"

  val PRINCIPAL_MAPPING_PLUGIN: String = "principalsMappingPlugin"
  val PRINCIPAL_MAPPING_PLUGIN_CLASS: String = s"$PRINCIPAL_MAPPING_PLUGIN.pluginClass"

  val IMPALA_PORT: String = "port"
  val IMPALA_SCHEMA: String = "schema"

  val HDFS: String = "hdfs"
  val HDFS_BASE_URL: String = "base-url"

  val RANGER: String = "ranger"
  val RANGER_AUTH_TYPE: String = "auth-type"
  val RANGER_USERNAME: String = "username"
  val RANGER_PASSWORD: String = "password"
  val RANGER_API_ENDPOINT: String = "base-url"

  val PRIVATE_CONFIG: String = "private-cloud"
  val COORDINATOR_HOST_URLS: String = s"$PRIVATE_CONFIG.coordinator-host-urls"

}
