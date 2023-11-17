package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger

import it.agilelab.provisioning.commons.client.ranger.model.{
  RangerResource,
  RangerSecurityZoneResources
}

object RangerResources {
  private val DB_KEY = "database"
  private val TABLE_KEY = "table"
  private val COLUMN_KEY = "column"
  private val URL_KEY = "url"

  def database(database: String): Map[String, RangerResource] = Map(
    DB_KEY -> RangerResource(Seq(database), isExcludes = false, isRecursive = false)
  )

  def table(database: String, table: String): Map[String, RangerResource] = Map(
    DB_KEY     -> RangerResource(Seq(database), isExcludes = false, isRecursive = false),
    TABLE_KEY  -> RangerResource(Seq(table), isExcludes = false, isRecursive = false),
    COLUMN_KEY -> RangerResource(Seq("*"), isExcludes = false, isRecursive = false)
  )

  def tableWithDbResourceExcluded(database: String, table: String): Map[String, RangerResource] =
    Map(
      DB_KEY     -> RangerResource(Seq(database), isExcludes = true, isRecursive = false),
      TABLE_KEY  -> RangerResource(Seq(table), isExcludes = false, isRecursive = false),
      COLUMN_KEY -> RangerResource(Seq("*"), isExcludes = false, isRecursive = false)
    )

  def url(url: String): Map[String, RangerResource] = Map(
    URL_KEY -> RangerResource(Seq(url), isExcludes = false, isRecursive = true)
  )

  def impalaSecurityZoneResources(
      database: Seq[String],
      column: Seq[String],
      table: Seq[String],
      url: Seq[String]
  ): RangerSecurityZoneResources =
    RangerSecurityZoneResources(
      Seq(
        Map(
          "database" -> database,
          "column"   -> column,
          "table"    -> table
        ),
        Map(
          "url" -> url
        )
      )
    )

}
