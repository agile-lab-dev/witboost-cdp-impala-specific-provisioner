<p align="center">
    <a href="https://www.agilelab.it/witboost">
        <img src="docs/img/witboost_logo.svg" alt="witboost" width=600 >
    </a>
</p>

Designed by [Agile Lab](https://www.agilelab.it/), witboost is a versatile platform that addresses a wide range of sophisticated data engineering challenges. It enables businesses to discover, enhance, and productize their data, fostering the creation of automated data platforms that adhere to the highest standards of data governance. Want to know more about witboost? Check it out [here](https://www.agilelab.it/witboost) or [contact us!](https://www.agilelab.it/contacts)

# CDW Impala Specific Provisioner

[![pipeline status](https://gitlab.com/AgileFactory/Witboost.Mesh/Provisioning/cdp-refresh/witboost.Mesh.Provisioning.OutputPort.CDP.Impala/badges/develop/pipeline.svg)](https://gitlab.com/AgileFactory/Witboost.Mesh/Provisioning/cdp-refresh/witboost.Mesh.Provisioning.OutputPort.CDP.Impala/-/commits/develop)  
[![coverage report](https://gitlab.com/AgileFactory/Witboost.Mesh/Provisioning/cdp-refresh/witboost.Mesh.Provisioning.OutputPort.CDP.Impala/badges/develop/coverage.svg)](https://gitlab.com/AgileFactory/Witboost.Mesh/Provisioning/cdp-refresh/witboost.Mesh.Provisioning.OutputPort.CDP.Impala/-/commits/develop)

- [Overview](#overview)
- [Building](#building)
- [Running](#running)
- [Configuring](#configuring)
- [Deploying](#deploying)
- [How it works](#how-it-works)
- [HLD](docs/HLD.md)
- [API specification](docs/API.md)

## Overview

This project implements a Specific Provisioner for Cloudera Data Warehouse using Impala and Amazon Web Services (AWS) S3 storage. After deploying this microservice and configuring witboost to use it, the platform can create Output Ports on existing csv or Parquet tables leveraging an existing Impala CDW environment.

### What's a Specific Provisioner?

A Specific Provisioner is a microservice which is in charge of deploying components that use a specific technology. When the deployment of a Data Product is triggered, the platform generates it descriptor and orchestrates the deployment of every component contained in the Data Product. For every such component the platform knows which Specific Provisioner is responsible for its deployment, and can thus send a provisioning request with the descriptor to it so that the Specific Provisioner can perform whatever operation is required to fulfill this request and report back the outcome to the platform.

You can learn more about how the Specific Provisioners fit in the broader picture [here](https://docs.witboost.agilelab.it/docs/p2_arch/p1_intro/#deploy-flow).

### Software stack

This microservice is written in Scala 2.13, using HTTP4s and Guardrail for the HTTP layer. Project is built with SBT and supports packaging as JAR, fat-JAR and Docker image, ideal for Kubernetes deployments (which is the preferred option).

This is a multi module sbt project:

* **api**: Contains the API layer of the service. The latter can be invoked synchronously in 3 different ways:
    1. POST /provision: provision the impala output port specified in the payload request. It will synchronously call the `service` logic to perform the provisioning logic.
    3. POST /validate: validate the payload request and return a validation result. It should be invoked before provisioning a resource in order to understand if the request is correct
* **core**: Contains model case classes and shared logic among the projects
* **service**: Contains the Provisioner Service logic. Is called from the API layer after some check on the request and return the deployed resource. This is the module on which we provision the output port

In this project we are using the following sbt plugins: 
1. **scalaformat**: To keep the scala style aligned with all collaborators
2. **wartRemover**: To keep the code as functional as possible
3. **scoverage**: To create a test coverage report
4. **k8tyGitlabPlugin**: To publish the packages to Gitlab Package Registry

### Artifacts

We produce two different artifacts on the CI/CD for this repository
1. The scoverage report that you could download from the CI/CD and check the test coverage
2. A docker image published in the Gitlab Container Registry
3. A set of jars, one for each module published in the Maven Gitlab Package Registry


## Building

**Requirements:**

- Java >=11
- sbt

This project depends on a private library scala-mesh-commons which you should have access to at compile time. Currently, the library is hosted as a package in a Gitlab Maven Package Registry.

To pull these libraries, we need to set up authentication to the Package Registry (see [Gitlab docs](https://docs.gitlab.com/ee/user/packages/maven_repository/?tab=sbt)). We've set authentication based on environment variables that sbt uses to authenticate. Please export the following environment variables before importing the project:

```shell
export GITLAB_ARTIFACT_HOST=https://gitlab.com/api/v4/projects/51107980/packages/maven
export GITLAB_ARTIFACT_USER=<Gitlab Username>
export GITLAB_ARTIFACT_TOKEN=<Gitlab Personal Access Token>
```

**Generating sources:** this project uses OpenAPI as standard API specification and the [sbt-guardrail](https://github.com/guardrail-dev/sbt-guardrail) plugin to generate server code from the [specification](./api/src/main/openapi/interface-specification.yml).

The code generation is done automatically in the compile phase:

```bash
sbt compile
```

### Test

**Tests:** are handled by the standard task as well:

```bash
sbt test
```

### CI/CD

Once you commit and push the CI/CD will be triggered, test and build phase are executed at each push. The CI/CD will use the job token to push the dependency libraries
Dev Deploy are executed only for master branch
Prod Deploy are executed only for release branch
You could double-check the artifacts that will be deployed downloading from the CI/CD artifacts.zip that was cached during the test/build stages

### How to collaborate

We recommend using IntelliJ IDEA Community Edition for developing this project.
You are free to use your favorite IDE. Please remember to add on the .gitignore the IDE specific files.

If you fork this repository, please modify the [project settings](./project/Settings.scala) with the appropriate gitlab project id to avoid trying pushing artifacts to the wrong repository.


### Scala style

Leverage the scalaformat library to reformat the code while editing. This will apply the scala format specification written on the `.scalafmt.conf` and avoids fake changes on merge request.

We added additional compilation rules using the wartRemover library, so if any exceptions are raised during compile time please fix them.

## Running

To run the server, you need to set up the necessary environment variables to access CDP and the AWS environment. This Specific Provisioner uses the followings SDK:

- **CDP SDK**: please refer to the [official documentation](https://docs.cloudera.com/cdp-public-cloud/cloud/sdk/topics/mc-overview-of-the-cdp-sdk-for-java.html) to setup the access credentials
- **AWS SDK**: please refer to the [official documentation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/setup-basics.html) to setup the access credentials

For example, for local execution you need to set the following environment variables:

```
export AWS_REGION=<aws_region>
export AWS_ACCESS_KEY_ID=<aws_access_key_id>
export AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
export AWS_SESSION_TOKEN=<aws_session_token>

export CDP_DEPLOY_ROLE_USER=<cdp_user>
export CDP_DEPLOY_ROLE_PASSWORD=<cdp_password>
export CDP_ACCESS_KEY_ID=<cdp_user_access_key_id>
export CDP_PRIVATE_KEY=<cdp_user_private_key>
```

The CDP user must be a `Machine User` and needs to have at least the following roles:
- DWAdmin
- DWUser
- EnvironmentAdmin
- EnvironmentUser


After this, execute:

```bash
sbt compile run
```

By default, the server binds to port 8080 on localhost. After it's up and running you can make provisioning requests to this address.

## Configuring

Most application configurations are handled with the Typesafe Config library. You can find the default settings in the `reference.conf` of each module. Customize them and use the `config.file` system property or the other options provided by Typesafe Config according to your needs. The provided docker image expects the config file mounted at path `/config/application.conf`.

For more information on the configuration, see [Configuring the Impala SP](docs/Configuration.md).

## Deploying

This microservice is meant to be deployed to a Kubernetes cluster.

## How it works

1. Parse the request body
2. Retrieve impala coordinator host and ranger host from cdpEnvironment specified on the request
3. Create the impala table
4. Upsert the ranger security zone for the specific data product version
5. Upsert ranger roles for owner and users of the component.
6. Upsert access policies for said roles, granting read/write access to the owner role, and read-only to the user role
7. Return the deployed resource

## Example input requested

The specific field shape must correspond to the format below. Specially, the database and table name should follow the convention:

- Database name: Impala Database name must be equal to `$Domain_$DataProductName_$MajorVersion` and must contain only the characters `[a-zA-Z0-9_]`. All other characters (like spaces or dashes) must be replaced with underscores (`_`).
- Table name: Impala Table name must be equal to `$Domain_$DPName_$DPMajorVrs_$ComponentName_$Environment` and must contain only the characters `[a-zA-Z0-9_]`. All other characters (like spaces or dashes) must be replaced with underscores (`_`).


Simple table:
```
id: urn:dmb:dp:platform:demo-dp:0
name: demo-dp
domain: platform
environment: dev
version: 0.0.1
dataProductOwner: dataProductOwner
specific: {}
components:
- id: urn:dmb:cmp:platform:demo-dp:0:witboost_table_impala
  name: witboost_table_impala
  description: description
  version: 0.0.1
  dataContract:
    schema:
    - name: id
      dataType: STRING
    - name: name
      dataType: STRING
    - name: surname
      dataType: STRING
  specific: 
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc
    cdpEnvironment: sdp-aw-d-ir-env1
    cdwVirtualWarehouse: sdpawdir-sfs-i
    format: CSV
    location: s3a://bucket/path/to/folder/
```

Partitions are defined as a list of column names to be used as partitions. These columns must exist in the `dataContract.schema` field and will be validated before deploying any resource.

Partitioned table:
```
id: urn:dmb:dp:platform:demo-dp:0
name: demo-dp
domain: platform
environment: dev
version: 0.0.1
dataProductOwner: dataProductOwner
specific: {}
components:
- id: urn:dmb:cmp:platform:demo-dp:0:witboost_table_impala
  name: witboost_table_impala
  description: description
  version: 0.0.1
  dataContract:
    schema:
    - name: id
      dataType: STRING
    - name: name
      dataType: STRING
    - name: country
      dataType: STRING
  specific: 
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc
    cdpEnvironment: sdp-aw-d-ir-env1
    cdwVirtualWarehouse: sdpawdir-sfs-i
    format: CSV
    location: s3a://bucket/path/to/folder/
    partitions:
      - country
```

## Limitations

* Only PARQUET and CSV are supported as `format`
  * CSV files must NOT start with a header row
* `location` need to be expressed as `s3a://` and always ending with a slash `/`.
  * Each Data Product data must be located in a different bucket 
* Schema data types only support primitive types TINYINT, SMALLINT, INT, BIGINT, DOUBLE, DECIMAL, TIMESTAMP, DATE, STRING, CHAR, VARCHAR and BOOLEAN, and ARRAY, MAP and STRUCT with these types

## Tech debt

* Improve code coverage
* Improve code quality
* Add Audit to other class

## About us

<p align="center">
    <a href="https://www.agilelab.it">
        <img src="docs/img/agilelab_logo.jpg" alt="Agile Lab" width=600>
    </a>
</p>

Agile Lab creates value for its Clients in data-intensive environments through customizable solutions to establish performance driven processes, sustainable architectures, and automated platforms driven by data governance best practices.

Since 2014 we have implemented 100+ successful Elite Data Engineering initiatives and used that experience to create Witboost: a technology agnostic, modular platform, that empowers modern enterprises to discover, elevate and productize their data both in traditional environments and on fully compliant Data mesh architectures.

[Contact us](https://www.agilelab.it/contacts) or follow us on:
- [LinkedIn](https://www.linkedin.com/company/agile-lab/)
- [Instagram](https://www.instagram.com/agilelab_official/)
- [YouTube](https://www.youtube.com/channel/UCTWdhr7_4JmZIpZFhMdLzAA)
- [Twitter](https://twitter.com/agile__lab)
