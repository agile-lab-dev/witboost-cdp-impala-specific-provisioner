package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone

import it.agilelab.provisioning.commons.client.ranger.model.{
  RangerSecurityZone,
  RangerSecurityZoneResources
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.RangerResources

object RangerSecurityZoneGenerator {
  def securityZone(
      zoneName: String,
      serviceName: String,
      databaseResources: Seq[String],
      tableResources: Seq[String],
      columnResources: Seq[String],
      urlResources: Seq[String],
      adminUsers: Seq[String],
      adminUserGroups: Seq[String],
      auditUsers: Seq[String],
      auditUserGroups: Seq[String]
  ): RangerSecurityZone =
    RangerSecurityZoneGenerator
      .empty(zoneName)
      .copy(
        services = Map(
          serviceName -> RangerResources.impalaSecurityZoneResources(
            database = databaseResources,
            column = columnResources,
            table = tableResources,
            url = urlResources)),
        adminUsers = adminUsers,
        adminUserGroups = adminUserGroups,
        auditUsers = auditUsers,
        auditUserGroups = auditUserGroups
      )

  def securityZoneWithMergedServiceResources(
      zone: RangerSecurityZone,
      serviceName: String,
      databaseResources: Seq[String],
      tableResources: Seq[String],
      columnResources: Seq[String],
      urlResources: Seq[String]
  ): RangerSecurityZone = zone.copy(
    services = zone.services.updatedWith(serviceName)(resources =>
      Some(
        resources.fold(
          RangerResources.impalaSecurityZoneResources(
            database = databaseResources,
            table = tableResources,
            column = columnResources,
            url = urlResources)
        )(old =>
          RangerResources.impalaSecurityZoneResources(
            database = mergeResources(old, databaseResources, "database"),
            table = mergeResources(old, tableResources, "table"),
            column = mergeResources(old, columnResources, "column"),
            url = mergeResources(old, urlResources, "url")
          ))
      ))
  )

  def securityZoneWithoutUrlResources(
      zone: RangerSecurityZone,
      serviceName: String,
      databaseResources: Seq[String],
      tableResources: Seq[String],
      columnResources: Seq[String],
      urlResources: Seq[String]
  ): RangerSecurityZone = zone.copy(
    services = zone.services.updatedWith(serviceName)(resources =>
      Some(
        resources.fold(
          RangerResources.impalaSecurityZoneResources(
            database = databaseResources,
            table = tableResources,
            column = columnResources,
            url = Seq.empty[String])
        )(old =>
          RangerResources.impalaSecurityZoneResources(
            database = mergeResources(old, databaseResources, "database"),
            table = mergeResources(old, tableResources, "table"),
            column = mergeResources(old, columnResources, "column"),
            url = old.resources
              .filter(res => res.contains("url"))
              .flatMap(m => m("url"))
              .filter(u => !urlResources.contains(u))
          ))
      ))
  )

  def empty(name: String): RangerSecurityZone =
    RangerSecurityZone(
      id = -1,
      name = name,
      services = Map.empty,
      isEnabled = true,
      adminUsers = Seq.empty,
      adminUserGroups = Seq.empty,
      auditUsers = Seq.empty,
      auditUserGroups = Seq.empty
    )

  private def mergeResources(
      oldRes: RangerSecurityZoneResources,
      newRes: Seq[String],
      resKey: String
  ): Seq[String] =
    oldRes.resources
      .filter(res => res.contains(resKey))
      .flatMap(m => m(resKey))
      .concat(newRes)
      .distinct

}
