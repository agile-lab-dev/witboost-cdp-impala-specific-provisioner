package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy

import it.agilelab.provisioning.commons.client.ranger.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.RangerResources
import org.scalatest.funsuite.AnyFunSuite

class RangerPolicyGeneratorTest extends AnyFunSuite {

  test("impalaTable") {
    val actual = RangerPolicyGenerator.impalaTable(
      database = "db",
      table = "tbl",
      ownerRole = "ownerRole",
      userRole = Some("userRole"),
      usersOwners = Seq("x"),
      "szName"
    )
    val expected = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("ownerRole"),
          groups = Seq.empty[String],
          users = Seq("x"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        ),
        RangerPolicyItem(
          roles = Seq("userRole"),
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

    assert(actual == expected)
  }

  test("impalaDb") {
    val actual = RangerPolicyGenerator.impalaDb(
      database = "db",
      ownerRole = "ownerRole",
      userRole = Some("userRole"),
      usersOwners = Seq("x", "y"),
      "szName"
    )
    val expected = RangerPolicy(
      -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = Map(
        "database" -> RangerResource(Seq("db"), isExcludes = false, isRecursive = false)
      ),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("ownerRole"),
          groups = Seq.empty[String],
          users = Seq("x", "y"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        ),
        RangerPolicyItem(
          roles = Seq("userRole"),
          groups = Seq.empty[String],
          users = Seq.empty,
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
    assert(actual == expected)
  }

  test("impalaUrl") {
    val actual = RangerPolicyGenerator.impalaUrl(
      database = "db",
      table = "tbl",
      url = "url",
      ownerRole = "ownerRole",
      userRole = Some("userRole"),
      usersOwners = Seq("x", "y"),
      "szName"
    )
    val expected = RangerPolicy(
      -1,
      service = "cm_hive",
      name = "db_tbl_url_access_policy",
      description = "db_tbl_url_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = Map(
        "url" -> RangerResource(Seq("url"), isExcludes = false, isRecursive = true)
      ),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("ownerRole"),
          groups = Seq.empty[String],
          users = Seq("x", "y"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        ),
        RangerPolicyItem(
          roles = Seq("userRole"),
          groups = Seq.empty[String],
          users = Seq.empty,
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
    assert(actual == expected)
  }

  test("merge policy items to policy") {

    val items = Seq(
      RangerPolicyItem(
        roles = Seq("ownerRole"),
        groups = Seq.empty[String],
        users = Seq("x", "y"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("userRole"),
        groups = Seq.empty[String],
        users = Seq.empty,
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.select,
          Access.read
        )
      )
    )

    // Access in lowercase as this is how Ranger sends it
    val toMergeItems = Seq(
      RangerPolicyItem(
        roles = Seq("ownerRole"),
        groups = Seq.empty[String],
        users = Seq("x", "y"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access("all", isAllowed = true)
        )
      ),
      RangerPolicyItem(
        roles = Seq("user2Role"),
        groups = Seq.empty[String],
        users = Seq.empty,
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access("select", isAllowed = true),
          Access("read", isAllowed = true)
        )
      )
    )

    val mergedItems = Seq(
      RangerPolicyItem(
        roles = Seq("ownerRole"),
        groups = Seq.empty[String],
        users = Seq("x", "y"),
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.all
        )
      ),
      RangerPolicyItem(
        roles = Seq("userRole"),
        groups = Seq.empty[String],
        users = Seq.empty,
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.select,
          Access.read
        )
      ),
      RangerPolicyItem(
        roles = Seq("user2Role"),
        groups = Seq.empty[String],
        users = Seq.empty,
        conditions = Seq.empty[String],
        delegateAdmin = false,
        accesses = Seq(
          Access.select,
          Access.read
        )
      )
    )

    val policy = RangerPolicy(
      -1,
      service = "cm_hive",
      name = "db_tbl_url_access_policy",
      description = "db_tbl_url_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = Map(
        "url" -> RangerResource(Seq("url"), isExcludes = false, isRecursive = true)
      ),
      policyItems = items,
      serviceType = "hive",
      policyLabels = Seq("autogenerated"),
      isDenyAllElse = true,
      "szName",
      0
    )

    val expected = policy.copy(policyItems = mergedItems)
    val actual = RangerPolicyGenerator.policyWithMergedPolicyItems(policy, toMergeItems)
    assert(actual == expected)
  }

  test("remove role from policy") {
    val policy = RangerPolicy(
      -1,
      service = "cm_hive",
      name = "db_tbl_url_access_policy",
      description = "db_tbl_url_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = Map(
        "url" -> RangerResource(Seq("url"), isExcludes = false, isRecursive = true)
      ),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("ownerRole", "userRole"),
          groups = Seq.empty[String],
          users = Seq("x", "y"),
          conditions = Seq.empty[String],
          delegateAdmin = false,
          accesses = Seq(
            Access.all
          )
        ),
        RangerPolicyItem(
          roles = Seq("userRole"),
          groups = Seq.empty[String],
          users = Seq.empty,
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

    val expected = RangerPolicy(
      -1,
      service = "cm_hive",
      name = "db_tbl_url_access_policy",
      description = "db_tbl_url_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = Map(
        "url" -> RangerResource(Seq("url"), isExcludes = false, isRecursive = true)
      ),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("ownerRole"),
          groups = Seq.empty[String],
          users = Seq("x", "y"),
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

    val actual = RangerPolicyGenerator.policyWithRemovedRole(policy, "userRole")
    assert(actual == expected)
  }

  test("impalaTable without userRole") {
    val actual = RangerPolicyGenerator.impalaTable(
      database = "db",
      table = "tbl",
      ownerRole = "ownerRole",
      userRole = None,
      usersOwners = Seq("x"),
      "szName"
    )
    val expected = RangerPolicy(
      id = -1,
      service = "cm_hive",
      name = "db_tbl_access_policy",
      description = "db_tbl_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = RangerResources.table("db", "tbl"),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("ownerRole"),
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

    assert(actual == expected)
  }

  test("impalaDb without userRole") {
    val actual = RangerPolicyGenerator.impalaDb(
      database = "db",
      ownerRole = "ownerRole",
      userRole = None,
      usersOwners = Seq("x", "y"),
      "szName"
    )
    val expected = RangerPolicy(
      -1,
      service = "cm_hive",
      name = "db_access_policy",
      description = "db_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = Map(
        "database" -> RangerResource(Seq("db"), isExcludes = false, isRecursive = false)
      ),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("ownerRole"),
          groups = Seq.empty[String],
          users = Seq("x", "y"),
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
    assert(actual == expected)
  }

  test("impalaUrl without userRole") {
    val actual = RangerPolicyGenerator.impalaUrl(
      database = "db",
      table = "tbl",
      url = "url",
      ownerRole = "ownerRole",
      userRole = None,
      usersOwners = Seq("x", "y"),
      "szName"
    )
    val expected = RangerPolicy(
      -1,
      service = "cm_hive",
      name = "db_tbl_url_access_policy",
      description = "db_tbl_url_access_policy",
      isAuditEnabled = true,
      isEnabled = true,
      resources = Map(
        "url" -> RangerResource(Seq("url"), isExcludes = false, isRecursive = true)
      ),
      policyItems = Seq(
        RangerPolicyItem(
          roles = Seq("ownerRole"),
          groups = Seq.empty[String],
          users = Seq("x", "y"),
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
    assert(actual == expected)
  }
}
