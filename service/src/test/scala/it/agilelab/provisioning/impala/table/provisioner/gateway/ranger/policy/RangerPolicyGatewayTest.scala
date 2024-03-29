package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy

import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.{
  Access,
  RangerPolicy,
  RangerPolicyItem
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.PolicyAttachment
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.RangerResources
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class RangerPolicyGatewayTest extends AnyFunSuite with MockFactory {

  test("upsertPolicies call update method if policy exists") {
    val rangerClient = mock[RangerClient]
    val rangerTablePolicyGateway = new RangerPolicyGateway(rangerClient)

    val oldPolicyItems = Seq(
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("user1CdpRole"),
        groups = Seq.empty[String],
        users = Seq.empty[String],
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.select
        )
      )
    )

    val mergedNewPolicyItems = Seq(
      RangerPolicyItem(
        roles = Seq("owner2CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("user1CdpRole"),
        groups = Seq.empty[String],
        users = Seq.empty[String],
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.select
        )
      ),
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      )
    )

    val oldPolicyItemsTbl = Seq(
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("user1CdpRole"),
        groups = Seq.empty[String],
        users = Seq.empty[String],
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.select,
          Access.read
        )
      )
    )

    val mergedNewPolicyItemsTbl = Seq(
      RangerPolicyItem(
        roles = Seq("owner2CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("user1CdpRole"),
        groups = Seq.empty[String],
        users = Seq.empty[String],
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.select,
          Access.read
        )
      ),
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      )
    )

    val dbAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.database("db"),
      policyItems = oldPolicyItems,
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      1
    )

    val tblAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = oldPolicyItemsTbl,
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      0
    )

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_access_policy", Some("szName"))
      .once()
      .returns(Right(Some(dbAccessPolicy.copy(id = 144))))

    (rangerClient.updatePolicy _)
      .expects(dbAccessPolicy.copy(id = 144, policyItems = mergedNewPolicyItems))
      .once()
      .returns(Right(dbAccessPolicy.copy(id = 144, policyItems = mergedNewPolicyItems)))

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_tbl_access_policy", Some("szName"))
      .once()
      .returns(Right(Some(tblAccessPolicy.copy(id = 145))))

    (rangerClient.updatePolicy _)
      .expects(tblAccessPolicy.copy(id = 145, policyItems = mergedNewPolicyItemsTbl))
      .once()
      .returns(Right(tblAccessPolicy.copy(id = 145, policyItems = mergedNewPolicyItemsTbl)))

    val actual = rangerTablePolicyGateway.upsertPolicies(
      "db",
      "tbl",
      "owner2CdpRole",
      Some("user1CdpRole"),
      Seq("x"),
      "szName"
    )

    val expected = Right(
      Seq(
        PolicyAttachment("144", "db_access_policy"),
        PolicyAttachment("145", "db_tbl_access_policy")
      )
    )

    assert(actual == expected)
  }

  test("upsertPolicies call create method if policy does not exist") {
    val rangerClient = mock[RangerClient]
    val rangerTablePolicyGateway = new RangerPolicyGateway(rangerClient)

    val dbAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.database("db"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("owner1CdpRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        ),
        RangerPolicyItem(
          roles = Seq("user1CdpRole"),
          groups = Seq.empty[String],
          users = Seq.empty[String],
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.select
          )
        )
      ),
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      1
    )

    val tblAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("owner1CdpRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        ),
        RangerPolicyItem(
          roles = Seq("user1CdpRole"),
          groups = Seq.empty[String],
          users = Seq.empty[String],
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.select,
            Access.read
          )
        )
      ),
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      0
    )

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_access_policy", Some("szName"))
      .once()
      .returns(Right(None))

    (rangerClient.createPolicy _)
      .expects(dbAccessPolicy)
      .once()
      .returns(Right(dbAccessPolicy.copy(id = 144)))

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_tbl_access_policy", Some("szName"))
      .once()
      .returns(Right(None))

    (rangerClient.createPolicy _)
      .expects(tblAccessPolicy)
      .once()
      .returns(Right(tblAccessPolicy.copy(id = 145)))

    val actual = rangerTablePolicyGateway.upsertPolicies(
      "db",
      "tbl",
      "owner1CdpRole",
      Some("user1CdpRole"),
      Seq("x"),
      "szName"
    )

    val expected = Right(
      Seq(
        PolicyAttachment("144", "db_access_policy"),
        PolicyAttachment("145", "db_tbl_access_policy")
      )
    )

    assert(actual == expected)
  }

  test("deletePolicies call delete method if policy exists") {
    val rangerClient = mock[RangerClient]
    val rangerTablePolicyGateway = new RangerPolicyGateway(rangerClient)

    val policyItems = Seq(
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("user1CdpRole"),
        groups = Seq.empty[String],
        users = Seq.empty[String],
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.select,
          Access.read
        )
      )
    )

    val tblAccessPolicy = RangerPolicy(
      id = 145,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = policyItems,
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      0
    )

    val dbAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.database("db"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("owner1CdpRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        ),
        RangerPolicyItem(
          roles = Seq("user1CdpRole"),
          groups = Seq.empty[String],
          users = Seq.empty[String],
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.select
          )
        )
      ),
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      1
    )

    // Removed userPolicyItem
    val dbUpdatedAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.database("db"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("owner1CdpRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        )
      ),
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      1
    )

    inSequence(
      (rangerClient.findPolicyByName _)
        .expects("cm_hive", "db_access_policy", Some("szName"))
        .once()
        .returns(Right(Some(dbAccessPolicy))),
      (rangerClient.updatePolicy _)
        .expects(dbUpdatedAccessPolicy)
        .once()
        .returns(Right(dbUpdatedAccessPolicy))
    )

    inSequence(
      (rangerClient.findPolicyByName _)
        .expects("cm_hive", "db_tbl_access_policy", Some("szName"))
        .once()
        .returns(Right(Some(tblAccessPolicy))),
      (rangerClient.deletePolicy _)
        .expects(tblAccessPolicy)
        .once()
        .returns(Right())
    )

    val actual = rangerTablePolicyGateway.deletePolicies(
      "db",
      "tbl",
      Some("user1CdpRole"),
      "szName"
    )

    val expected = Right(
      Seq(
        PolicyAttachment("145", "db_tbl_access_policy")
      )
    )

    assert(actual == expected)
  }

  test("deletePolicies does not call delete method if policy does not exist") {
    val rangerClient = mock[RangerClient]
    val rangerTablePolicyGateway = new RangerPolicyGateway(rangerClient)

    val tblAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("owner1CdpRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        ),
        RangerPolicyItem(
          roles = Seq("user1CdpRole"),
          groups = Seq.empty[String],
          users = Seq.empty[String],
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.select,
            Access.read
          )
        )
      ),
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      0
    )

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_access_policy", Some("szName"))
      .once()
      .returns(Right(None))

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_tbl_access_policy", Some("szName"))
      .once()
      .returns(Right(None))

    val actual = rangerTablePolicyGateway.deletePolicies(
      "db",
      "tbl",
      Some("user1CdpRole"),
      "szName"
    )

    val expected = Right(
      Seq.empty[PolicyAttachment]
    )

    assert(actual == expected)
  }

  test("upsertPolicies call update method if policy exists excluding userRole if is None") {
    val rangerClient = mock[RangerClient]
    val rangerTablePolicyGateway = new RangerPolicyGateway(rangerClient)

    val oldPolicyItems = Seq(
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      )
    )

    val mergedNewPolicyItems = Seq(
      RangerPolicyItem(
        roles = Seq("owner2CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      )
    )

    val oldPolicyItemsTbl = Seq(
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      )
    )

    val mergedNewPolicyItemsTbl = Seq(
      RangerPolicyItem(
        roles = Seq("owner2CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      )
    )

    val dbAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.database("db"),
      policyItems = oldPolicyItems,
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      1
    )

    val tblAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = oldPolicyItemsTbl,
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      0
    )

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_access_policy", Some("szName"))
      .once()
      .returns(Right(Some(dbAccessPolicy.copy(id = 144))))

    (rangerClient.updatePolicy _)
      .expects(dbAccessPolicy.copy(id = 144, policyItems = mergedNewPolicyItems))
      .once()
      .returns(Right(dbAccessPolicy.copy(id = 144, policyItems = mergedNewPolicyItems)))

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_tbl_access_policy", Some("szName"))
      .once()
      .returns(Right(Some(tblAccessPolicy.copy(id = 145))))

    (rangerClient.updatePolicy _)
      .expects(tblAccessPolicy.copy(id = 145, policyItems = mergedNewPolicyItemsTbl))
      .once()
      .returns(Right(tblAccessPolicy.copy(id = 145, policyItems = mergedNewPolicyItemsTbl)))

    val actual = rangerTablePolicyGateway.upsertPolicies(
      "db",
      "tbl",
      "owner2CdpRole",
      None,
      Seq("x"),
      "szName"
    )

    val expected = Right(
      Seq(
        PolicyAttachment("144", "db_access_policy"),
        PolicyAttachment("145", "db_tbl_access_policy")
      )
    )

    assert(actual == expected)
  }

  test("upsertPolicies call create method if policy does not exist excluding userRole if is None") {
    val rangerClient = mock[RangerClient]
    val rangerTablePolicyGateway = new RangerPolicyGateway(rangerClient)

    val dbAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.database("db"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("owner1CdpRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        )
      ),
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      1
    )

    val tblAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("owner1CdpRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        )
      ),
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      0
    )

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_access_policy", Some("szName"))
      .once()
      .returns(Right(None))

    (rangerClient.createPolicy _)
      .expects(dbAccessPolicy)
      .once()
      .returns(Right(dbAccessPolicy.copy(id = 144)))

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_tbl_access_policy", Some("szName"))
      .once()
      .returns(Right(None))

    (rangerClient.createPolicy _)
      .expects(tblAccessPolicy)
      .once()
      .returns(Right(tblAccessPolicy.copy(id = 145)))

    val actual = rangerTablePolicyGateway.upsertPolicies(
      "db",
      "tbl",
      "owner1CdpRole",
      None,
      Seq("x"),
      "szName"
    )

    val expected = Right(
      Seq(
        PolicyAttachment("144", "db_access_policy"),
        PolicyAttachment("145", "db_tbl_access_policy")
      )
    )

    assert(actual == expected)
  }

  test("deletePolicies call delete method if policy exists excluding userRole if is None") {
    val rangerClient = mock[RangerClient]
    val rangerTablePolicyGateway = new RangerPolicyGateway(rangerClient)

    val policyItems = Seq(
      RangerPolicyItem(
        roles = Seq("owner1CdpRole"),
        groups = Seq.empty[String],
        users = Seq("x"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      )
    )

    val tblAccessPolicy = RangerPolicy(
      id = 145,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = policyItems,
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      0
    )

    val dbAccessPolicy = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.database("db"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("owner1CdpRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        )
      ),
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      1
    )

    (rangerClient.findPolicyByName _)
      .expects("cm_hive", "db_access_policy", Some("szName"))
      .once()
      .returns(Right(Some(dbAccessPolicy)))

    inSequence(
      (rangerClient.findPolicyByName _)
        .expects("cm_hive", "db_tbl_access_policy", Some("szName"))
        .once()
        .returns(Right(Some(tblAccessPolicy))),
      (rangerClient.deletePolicy _)
        .expects(tblAccessPolicy)
        .once()
        .returns(Right())
    )

    val actual = rangerTablePolicyGateway.deletePolicies(
      "db",
      "tbl",
      None,
      "szName"
    )

    val expected = Right(
      Seq(
        PolicyAttachment("145", "db_tbl_access_policy")
      )
    )

    assert(actual == expected)
  }

}
