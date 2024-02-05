package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import com.typesafe.config.Config
import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamGroup, CdpIamUser }
import org.ldaptive._
import pureconfig._
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.generic.auto.exportReader

import java.time.Duration

final case class LdapConfig(
    url: String,
    useTls: Boolean,
    timeout: Int,
    bindUsername: String,
    bindPassword: String,
    searchBaseDN: String,
    userSearchFilter: String,
    groupSearchFilter: String,
    userAttributeName: String,
    groupAttributeName: String
)

object LdapConfig {
  def fromConfig(config: Config): Either[ConfigReaderFailures, LdapConfig] =
    ConfigSource.fromConfig(config).load[LdapConfig]

  implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))
}

trait LdapClient {
  def findUserByMail(mail: String): Either[LdapClientError, Option[CdpIamUser]]
  def findGroupByName(name: String): Either[LdapClientError, Option[CdpIamGroup]]
}

object LdapClient {
  def default(searchOperation: SearchOperation, ldapConfig: LdapConfig): LdapClient =
    new DefaultLdapClient(searchOperation = searchOperation, ldapConfig = ldapConfig)

  def defaultWithAudit(searchOperation: SearchOperation, ldapConfig: LdapConfig): LdapClient =
    new DefaultLdapClientWithAudit(
      Audit.default("LdapClient"),
      LdapClient.default(searchOperation, ldapConfig)
    )

  def connectionFactory(ldapConfig: LdapConfig): ConnectionFactory = {
    val connectionFactory = PooledConnectionFactory.builder
      .config(
        ConnectionConfig.builder
          .url(ldapConfig.url)
          .useStartTLS(ldapConfig.useTls)
          .responseTimeout(Duration.ofMillis(ldapConfig.timeout))
          .connectionInitializers(BindConnectionInitializer.builder
            .dn(ldapConfig.bindUsername)
            .credential(ldapConfig.bindPassword)
            .build)
          .build)
      .min(1)
      .max(5)
      .build
    connectionFactory.initialize()
    connectionFactory
  }

  def searchOperation(ldapConfig: LdapConfig): SearchOperation = new SearchOperation(
    LdapClient.connectionFactory(ldapConfig))
}
