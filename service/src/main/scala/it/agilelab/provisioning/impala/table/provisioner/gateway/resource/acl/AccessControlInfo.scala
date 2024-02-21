package it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl

/** Access control information of the data product required to create the necessary permissions to handle the provisioned resources.
  * @param dataProductOwner Owner of the data product. It should be an User in the form expected by the PrincipalsMapper
  * @param devGroup Owner of the data product. It should be a Group in the form expected by the PrincipalsMapper
  * @param componentId Component id in the form urn:dmb:cmp:$domain:$name:$majorVersion:$componentName
  */
final case class AccessControlInfo(dataProductOwner: String, devGroup: String, componentId: String)
