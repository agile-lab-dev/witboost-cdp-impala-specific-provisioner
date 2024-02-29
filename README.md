<br/>
<p align="center">
    <a href="https://www.agilelab.it/witboost">
        <img src="docs/img/witboost_logo.svg" alt="witboost" width=600 >
    </a>
</p>
<br/>

Designed by [Agile Lab](https://www.agilelab.it/), Witboost is a versatile platform that addresses a wide range of sophisticated data engineering challenges. It enables businesses to discover, enhance, and productize their data, fostering the creation of automated data platforms that adhere to the highest standards of data governance. Want to know more about Witboost? Check it out [here](https://www.agilelab.it/witboost) or [contact us!](https://www.agilelab.it/contacts)

This repository is part of our [Starter Kit](https://github.com/agile-lab-dev/witboost-starter-kit) meant to showcase Witboost's integration capabilities and provide a "batteries-included" product.

# CDP Impala Specific Provisioner

[![pipeline status](https://gitlab.com/AgileFactory/Witboost.Mesh/Provisioning/cdp-refresh/witboost.Mesh.Provisioning.OutputPort.CDP.Impala/badges/master/pipeline.svg)](https://gitlab.com/AgileFactory/Witboost.Mesh/Provisioning/cdp-refresh/witboost.Mesh.Provisioning.OutputPort.CDP.Impala/-/commits/master)  
[![coverage report](https://gitlab.com/AgileFactory/Witboost.Mesh/Provisioning/cdp-refresh/witboost.Mesh.Provisioning.OutputPort.CDP.Impala/badges/master/coverage.svg?min_medium=60)](https://gitlab.com/AgileFactory/Witboost.Mesh/Provisioning/cdp-refresh/witboost.Mesh.Provisioning.OutputPort.CDP.Impala/-/commits/master)

- [Overview](#overview)
- [Building](#building)
- [Running](#running)
- [Configuring](#configuring)
- [Deploying](#deploying)
- [How it works](#how-it-works)
- [HLD](docs/HLD.md)
- [API specification](docs/API.md)

## Overview

This project implements a Specific Provisioner deploying Output Ports (as External Tables or Views) and Storage Areas* on Apache Impala hosted on a Cloudera Data Platform environment. It supports both CDP Public Cloud with Cloudera Data Warehouse (CDW) using Impala and Amazon Web Services (AWS) S3 storage, and CDP Private Cloud using Impala and HDFS. After deploying this microservice and configuring witboost to use it, the platform can create Output Ports and Storage Areas* on existing csv or Parquet tables leveraging an existing Impala instance.

> As of now, this provisioner can only deploy View Output Ports and Storage Areas on CDP Private Cloud environments.

### What's a Specific Provisioner?

A Specific Provisioner is a microservice which is in charge of deploying components that use a specific technology. When the deployment of a Data Product is triggered, the platform generates it descriptor and orchestrates the deployment of every component contained in the Data Product. For every such component the platform knows which Specific Provisioner is responsible for its deployment, and can thus send a provisioning request with the descriptor to it so that the Specific Provisioner can perform whatever operation is required to fulfill this request and report back the outcome to the platform.

You can learn more about how the Specific Provisioners fit in the broader picture [here](https://docs.witboost.agilelab.it/docs/p2_arch/p1_intro/#deploy-flow).

### Software stack

This microservice is written in Scala 2.13, using HTTP4s and Guardrail for the HTTP layer. Project is built with SBT and supports packaging as JAR, fat-JAR and Docker image, ideal for Kubernetes deployments (which is the preferred option).

This is a multi module sbt project:

* **api**: Contains the API layer of the service. The latter can be invoked synchronously in 3 different ways:
    1. POST /provision: provision the impala output port/storage area specified in the payload request. It will synchronously call the `service` logic to perform the provisioning logic.
    2. POST /validate: validate the payload request and return a validation result. It should be invoked before provisioning a resource in order to understand if the request is correct.
    3. POST /updateacl: Updates the access to users to the provisioned resources, only for output ports. 
* **core**: Contains model case classes and shared logic among the projects
* **service**: Contains the Provisioner Service logic. Is called from the API layer after some check on the request and return the deployed resource. This is the module on which we provision the output port/storage area

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

- **CDP SDK**: please refer to the [official documentation](https://docs.cloudera.com/cdp-public-cloud/cloud/sdk/topics/mc-overview-of-the-cdp-sdk-for-java.html) to setup the access credentials (only required for CDP Public Cloud).
- **AWS SDK**: please refer to the [official documentation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/setup-basics.html) to setup the access credentials (only required for CDP Public Cloud).

For example, for local execution you need to set the following environment variables:

```
# AWS configuration is only required for CDP Public Cloud
export AWS_REGION=<aws_region>
export AWS_ACCESS_KEY_ID=<aws_access_key_id>
export AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
export AWS_SESSION_TOKEN=<aws_session_token>

export CDP_DEPLOY_ROLE_USER=<cdp_user>
export CDP_DEPLOY_ROLE_PASSWORD=<cdp_password>
export CDP_ACCESS_KEY_ID=<cdp_user_access_key_id> # Only required for CDP Public Cloud
export CDP_PRIVATE_KEY=<cdp_user_private_key> # Only required for CDP Public Cloud
```

This provisioner uses two sets of credentials to perform operations on Apache Ranger and Apache Impala. The default configuration sets them both equal to the environment variables `CDP_DEPLOY_ROLE_USER` and `CDP_DEPLOY_ROLE_PASSWORD`, so that only one user is initially necessary, but the Ranger credentials can be overridden via configuration if they need to be different (see [Configuring](#configuring)).

The used CDP users must be `Machine User` and need to check some requirements depending on the type of CDP Cloud.

### CDP Public Cloud

On **CDP Public** it needs to have at least the following roles:
- Impala:
  - DWAdmin
  - DWUser
- Ranger:
  - EnvironmentAdmin
  - EnvironmentUser
  
Alternatively to `EnvironmentAdmin` role, the Machine User for Ranger must have the necessary permissions to manage Ranger, specifically creating/updating/retrieving/deleting Security Zones, Roles, Resource based Policies and the resources related to them. If the same user is used for both services, it must have the four roles and/or permissions.

### CDP Private Cloud

On **CDP Private**, the deploy user needs to have admin privileges on Ranger, as well as have the following permissions (e.g. through Ranger policies):
- `read`, `write`, `execute` permissions on HDFS directory to be used
- `all` permissions on Impala databases and tables to be used
  
However, if Impala is authenticated using Kerberos as it is in most cases, the only set of credentials needed will be used to access Ranger, whereas for Impala a valid keytab with a principal with service name `impala` will be necessary, accompanied by the necessary kerberos configuration files (see [Configuring](#configuring)). 

After this, execute:

```bash
sbt compile run
```

By default, the server binds to port 8093 on localhost. After it's up and running you can make provisioning requests to this address.

## Configuring

Most application configurations are handled with the Typesafe Config library. You can find the default settings in the `reference.conf` of each module. Customize them and use the `config.file` system property or the other options provided by Typesafe Config according to your needs. The provided docker image expects the config file mounted at path `/config/application.conf`.

Especially for CDP Private Cloud, a set of required configuration fields must be modified, like Ranger and HDFS base URLs. For more information on the configuration and to understand how to set up the provisioner for a specific type of CDP Cloud, see [Configuring the Impala Specific Provisioner](docs/Configuration.md).

### Helm chart configuration

#### CDP Public v.s. CDP Private

The chart provides a couple of configurations to setup the provisioner to work on either CDP Public Cloud or CDP Private Cloud. `private.enabled` would set the necessary environment variables that the provisioner needs in order to work (see [Running](#running)). By setting it to `true` it will remove the Access Key and Private Key used by the Cloudera SDK to contact the public cloud.

The second configuration `kerberos.enabled` would set the necessary system properties needed for the provisioner to authenticate on a Kerberos system to services like Impala. For this, the provisioner expects a `jaas.conf` file and `krb5.conf`. For more information about these files see [Configuring the Impala Specific Provisioner](./docs/Configuration.md#jdbc-configuration). You can provide override values for these files using the `kerberos.krb5Override` and `kerberos.jaasOverride` fields.

#### Custom Root CA

The chart provides the option `customCA.enabled` to add a custom Root Certification Authority to the JVM truststore. If this option is enabled, the chart will load the custom CA from a secret with key `cdp-private-impala-custom-ca`. The CA is expected to be in a format compatible with `keytool` utility (PEM works fine).

## Deploying

This microservice is meant to be deployed to a Kubernetes cluster.

## How it works

1. Parse the request body
2. Retrieve impala coordinator host and ranger host from either the CDP environment (CDP Public), or the provisioner configuration (CDP Private).
3. Create the impala resource (table or view)
4. Upsert the ranger security zone for the specific data product version
5. Upsert ranger roles for owners of the component; and for Output Ports a role for users as well.
6. Upsert access policies for said roles, granting read/write access to the owner role, and read-only to the user role
7. Return the deployed resource

## Descriptor Input

The Impala Specific Provisioner receives a yaml-descriptor containing a data contract schema and a specific field with the information of the table or view to be deployed. It allows defining

- Data contract schema. OpenMetadata Column schema defining the schema of the table or view to be created
- Database name: Database to be created to handle the component tables
- Table name: Table name to be created, or when provisioning a view, the name of the table exposed by the view
- View name: Sent when provisioning a view to define its name
- Format: Format of the data files an external table exposes. Only required for table creation
- Location: Location in S3 (CDP Public) or HDFS (CDP Private) where the data files are located
- Partitions: List of columns used to partition the data
- Table parameters: Extra table parameters to define TBLPROPERTIES, text file delimiter and header, etc.

For the specification of schema of this object, check out [Descriptor Input](docs/DescriptorInput.md)

## License

This project is available under the [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0); see [LICENSE](LICENSE) for full details.

## About us
<br/>
<p align="center">
    <a href="https://www.agilelab.it">
        <img src="docs/img/agilelab_logo.svg" alt="Agile Lab" width=600>
    </a>
</p>
<br/>

Agile Lab creates value for its Clients in data-intensive environments through customizable solutions to establish performance driven processes, sustainable architectures, and automated platforms driven by data governance best practices.

Since 2014 we have implemented 100+ successful Elite Data Engineering initiatives and used that experience to create Witboost: a technology agnostic, modular platform, that empowers modern enterprises to discover, elevate and productize their data both in traditional environments and on fully compliant Data mesh architectures.

[Contact us](https://www.agilelab.it/contacts) or follow us on:
- [LinkedIn](https://www.linkedin.com/company/agile-lab/)
- [Instagram](https://www.instagram.com/agilelab_official/)
- [YouTube](https://www.youtube.com/channel/UCTWdhr7_4JmZIpZFhMdLzAA)
- [Twitter](https://twitter.com/agile__lab)
