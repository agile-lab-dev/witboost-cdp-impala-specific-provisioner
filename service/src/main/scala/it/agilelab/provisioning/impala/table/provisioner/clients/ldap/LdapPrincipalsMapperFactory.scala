package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import com.typesafe.config.Config
import it.agilelab.provisioning.commons.config.ConfError.ConfDecodeErr
import it.agilelab.provisioning.commons.principalsmapping.{
  CdpIamPrincipals,
  PrincipalsMapper,
  PrincipalsMapperFactory
}

import scala.util.{ Failure, Success, Try }

class LdapPrincipalsMapperFactory extends PrincipalsMapperFactory[CdpIamPrincipals] {
  override def create(config: Config): Try[PrincipalsMapper[CdpIamPrincipals]] =
    LdapConfig.fromConfig(config) match {
      case Right(ldapConfig) =>
        Success(
          new LdapPrincipalsMapper(
            LdapClient.defaultWithAudit(LdapClient.searchOperation(ldapConfig), ldapConfig)))
      case Left(error) => Failure(ConfDecodeErr(error.toString()))
    }

  override def configIdentifier: String = "ldap"
}
