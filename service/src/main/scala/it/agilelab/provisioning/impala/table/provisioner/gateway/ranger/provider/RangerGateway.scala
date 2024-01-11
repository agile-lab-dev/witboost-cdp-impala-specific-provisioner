package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider

import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy.RangerPolicyGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role.RangerRoleGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGateway

/** Groups classes needed to interact with Ranger
  * @param securityZoneGateway class to interact with Ranger zones
  * @param roleGateway class to interact with Ranger roles
  * @param policyGateway class to interact with Ranger policies
  */

class RangerGateway(
    val policyGateway: RangerPolicyGateway,
    val securityZoneGateway: RangerSecurityZoneGateway,
    val roleGateway: RangerRoleGateway
) {}
