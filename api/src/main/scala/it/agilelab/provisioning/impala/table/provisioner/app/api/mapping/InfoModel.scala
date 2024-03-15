package it.agilelab.provisioning.impala.table.provisioner.app.api.mapping

final case class InfoModel(`type`: String, label: String, value: String, href: Option[String])

object InfoModel {
  def makeStringInfoObject(label: String, value: String, href: Option[String] = None): InfoModel =
    InfoModel("string", label, value, href)
}
