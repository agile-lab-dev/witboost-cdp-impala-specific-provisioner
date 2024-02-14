package it.agilelab.provisioning.impala.table.provisioner.context

trait ConfigurationModel {
  val PROVISIONER: String = "provisioner"
  val PROVISION_CLOUD_TYPE: String = "provision-cloud"

  private val networking: String = "networking"
  private val httpServer: String = "httpServer"
  private val interface: String = "interface"
  private val port: String = "port"

  val NETWORKING_HTTPSERVER_INTERFACE: String = s"$networking.$httpServer.$interface"
  val NETWORKING_HTTPSERVER_PORT: String = s"$networking.$httpServer.$port"

  val IMPALA: String = "impala"

  val DROP_ON_UNPROVISION: String = "drop-on-unprovision"

  val PRINCIPAL_MAPPING_PLUGIN: String = "principalsMappingPlugin"
  val PRINCIPAL_MAPPING_PLUGIN_CLASS: String = "pluginClass"

  val JDBC_CONFIG: String = "jdbc"
  val JDBC_AUTH_TYPE: String = "auth-type"

  val JDBC_SIMPLE_AUTH: String = "simple"
  val JDBC_KERBEROS_AUTH: String = "kerberos"

  val JDBC_PORT: String = "port"
  val JDBC_SCHEMA: String = "schema"
  val JDBC_SSL: String = "ssl"
  val JDBC_KRBREALM: String = "KrbRealm"
  val JDBC_KRBHOSTFQDN: String = "KrbHostFQDN"
  val JDBC_KRBSERVICENAME: String = "KrbServiceName"

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
