# Configuring the Impala Specific Provisioner

Most application configurations are handled with the Typesafe Config library. You can find the default settings in the `reference.conf` of each module. Customize them and use the `config.file` system property or the other options provided by Typesafe Config according to your needs. The provided docker image expects the config file mounted at path `/config/application.conf`.

## Provisioner configuration

| Configuration                                 | Description                                                                                                                                                                                                                                                                      | Default   |
|:----------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------|
| `provisioner.networking.httpServer.interface` | Interface to bind the specific provisioner API layer                                                                                                                                                                                                                             | `0.0.0.0` |
| `provisioner.networking.httpServer.port`      | Port to bind the specific provisioner API layer                                                                                                                                                                                                                                  | `8093`    |
| `provisioner.provision-cloud`                 | Type of CDP environment that sets the tasks done by the provisioner                                                                                                                                                                                                              | `public`  |
| `provisioner.retry-config`                    | Configuration map for setting up exponential backoff strategy. If set, the provisioner will retry requests that otherwise would end up as a `500 Internal Server Error`                                                                                                          |           |
| `provisioner.provision-info`                  | Map of values corresponding to the public info to be returned by the provisioner as part of the deployment result. The schema should match with the one expected by the Witboost Marketplace. See the "Infrastructure Template" section of Witboost documentation for more info. | `{}`      |

### Request retry configuration

The Impala Specific Provisioner is capable of retrying requests using an exponential backoff strategy. If set, whenever a request would end up returning `500 Internal Server Error`, the provisioner will instead retry it up to `provisioner.retry-config.max-retries` times. Among each retry, the delay time will be set as double the previous one, with initial value `provisioner.retry-config.exponential-backoff` (for instantaneous retries, set this value to 0). The request will be retried in its entirety, leveraging the idempotency of the provisioner, to ensure upright behaviour.

| Configuration                      | Description                                                            | Default            |
|:-----------------------------------|:-----------------------------------------------------------------------|:-------------------|
| `retry-config.max-retries`         | The amount of times the provisioner will re-attempt a certain request  | `5`                |
| `retry-config.exponential-backoff` | The delay between retries, doubling the previous time for each attempt | `500 milliseconds` | 

Example:

```
provisioner {
  networking {
    httpServer {
      port: 8080
      interface: "0.0.0.0"
    }
  }
  
  provision-cloud = private
  
  retry-config {
    max-retries = 5
    exponential-backoff = 500 milliseconds
  }
  
  provision-info {
    "hueLink" {
      type: "string"
      label: "Field label"
      value: "Showable clickable"
      href: "http://hue.internal"
    }
    "otherInfo" {
      type: "string"
      label: "ValuableInfo"
      value: "Showable value"
    }
  }
}
```


## Service configuration

| Configuration                                | Description                                                                                                                                         | Default |
|:---------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------|:--------|
| `impala.jdbc`                                | Contains the information about the jdbc connection string to be used to communicate with Impala. See [JDBC configuration](#jdbc-configuration)      |
| `impala.drop-on-unprovision`                 | Drops the created external tables at unprovision time                                                                                               | `true`  |
| `impala.private-cloud.coordinator-host-urls` | List of Impala coordinator hosts available to receive JDBC connections. Only required if `provisioner.provision-cloud = private`.                   | `[]`    |

### JDBC Configuration

This Specific Provisioner uses the [Cloudera Impala JDBC driver](https://www.cloudera.com/downloads/connectors/impala/jdbc/2-6-32.html) to create connections to the Impala service. The following explains how configure the type of JDBC connection string to be used. Currently, the provisioner supports two types of JDBC connection string: Username/Password, and Kerberos.

| Configuration    | Description                                                                                                                    | Default   |
|:-----------------|:-------------------------------------------------------------------------------------------------------------------------------|:----------|
| `jdbc.auth-type` | Type of JDBC connection to be used. Allowed values: `simple \| kerberos`. `simple` stands for Username/Password authentication | `simple`  |
| `jdbc.port`      | Port to be used to establish JDBC connections to Impala*                                                                       | `443`     |
| `jdbc.schema`    | Schema to be used on the JDBC connection string to Impala                                                                      | `default` |
| `jdbc.ssl`       | Enables or disables SSL when establishing the connection to Impala                                                             | `true`    |

> *=Currently, for Username/Password JDBC connection strings, only HTTP(S) connections are allowed, as currently the JDBC `transportMode` is set to `http`-only for this type of connection.

For Kerberos authentication, the following fields should also be configured:

| Configuration         | Description                                                                                                                        | Default |
|:----------------------|:-----------------------------------------------------------------------------------------------------------------------------------|:--------|
| `jdbc.KrbRealm`       | Kerberos realm of the Impala server. If not set, the default realm of the microservice's Kerberos configuration will be used       |         |
| `jdbc.KrbHostFQDN`    | Fully qualified domain name of the Impala server host. If not set, it will attempt connection by setting it equal to the host name |         |
| `jdbc.KrbServiceName` | Kerberos service name. Typically equal to `impala`                                                                                 |         |

Example of Kerberos configuration:
```
impala {
  ...
  jdbc {
    auth-type = "kerberos"
    port = 21050
    schema = default
    KrbRealm = "REALM" # Optional. If not set, default value from krb5.conf one will be used
    KrbHostFQDN = "FQDN.HOST" # Optional. If not set -> = host
    KrbServiceName = "impala"
    ssl = true
  }
}
```

In order to allow the provisioner to authenticate against Kerberos, it is necessary to include into the microservice a `krb5.conf` with the setup configuration, as well as a `jaas.conf` configuration file for the Java Authentication and Authorization Service (JAAS) library. Examples of these two files can be found [here](../helm/files/).

Furthermore, you'll need to set the following system property values when executing the provisioner:

```
-Djava.security.krb5.conf=PATH/TO/krb5.conf \
-Djava.security.auth.login.config=PATH/TO/jaas.conf
```

Only set the `java.security.krb5.conf` system property if the `krb5.conf` is not put into a [default location](https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html).

### Principals Mapper configuration

In order to map Witboost users and groups to the target environment, a Principals Mapper is used. This sections explains how to configure it in order to connect to the target environment.

| Configuration                         | Description                                                            | Default |
|:--------------------------------------|:-----------------------------------------------------------------------|:--------|
| `principalsMappingPlugin.pluginClass` | Fully qualified name of the PrincipalsMapperFactory to be instantiated |         |

Each PrincipalsMapperFactory may require additional configuration information to create the PrincipalsMapper instance. This configuration is retrieved from an object in the `principalsMappingPlugin` config object. The key of this object shall be equal to the `configIdentifier` of the instantiated PrincipalsMapperFactory. **Even if the PrincipalsMapper doesn't require additional configuration, the key must be present as an empty object**.

#### FreeIpaIdentityPrincipalsMapper

PrincipalsMapper to query the FreeIPA instance of a CDP Public Cloud leveraging the Cloudera SDK.

Example of configuration to load `FreeIpaIdentityPrincipalsMapper`:

```
principalsMappingPlugin {
  pluginClass = "it.agilelab.provisioning.commons.principalsmapping.impl.freeipa.FreeIpaIdentityPrincipalsMapperFactory"
  freeipa-identity {}
}
```

#### LdapPrincipalsMapper

PrincipalsMapper to query an LDAP instance and map the principals to either users or groups based on the provided configuration.

| Configuration             | Description                      | 
|:--------------------------|:---------------------------------|
| `ldap.url`                | LDAP url                         |
| `ldap.useTls`             | TLS enable flag                  |
| `ldap.timeout`            | Timeout in milliseconds          |
| `ldap.bindUsername`       | Bind user                        |
| `ldap.bindPassword`       | Bind password.                   |
| `ldap.searchBaseDN`       | Base DN                          |
| `ldap.userSearchFilter`   | Ldap filter for user search      |
| `ldap.groupSearchFilter`  | Ldap filter for group search     |
| `ldap.userAttributeName`  | Ldap attribute name for user Id  |
| `ldap.groupAttributeName` | Ldap attribute name for group Id |

Example of configuration to load `LdapPrincipalsMapper` where the password is retrieved through an environment variable:

```
principalsMappingPlugin {
  "pluginClass" = "it.agilelab.provisioning.impala.table.provisioner.clients.ldap.LdapPrincipalsMapperFactory"
  ldap {
    url: "ldap://localhost:389"
    useTls: false
    timeout: 30000
    bindUsername: "user"
    bindPassword: ${?LDAP_BIND_PASSWORD}
    searchBaseDN: "DC=agilelab,DC=it"
    userSearchFilter: "(mail={mail})"
    groupSearchFilter: "(&(objectClass=groupOfNames)(cn={group}))"
    userAttributeName: "cn"
    groupAttributeName: "cn"
  }
}
```

## HDFS configuration

Configuration used only on CDP Private environments (`provision-cloud = private`). Left empty if CDP Public with S3 is being used.

> This feature is currently disabled

| Configuration   | Description                                            | Default |
|:----------------|:-------------------------------------------------------|:--------|
| `hdfs.base-url` | WebHDFS base URL in the form `http[s]://<HOST>:<PORT>` |         |

Example:

```
hdfs {
    base-url = "http://localhost:50070"
}
```

## Ranger configuration

Ranger configuration is used to define the authentication method to access the platform. On CDP Private Cloud environment it also requires to define the base URL to access, while on CDP Public Cloud this is automatically discovered. This configuration gives the possibility to define different credentials for deploying and for accessing Ranger by overriding the username and password fields.

| Configuration      | Description                                                                                                                                                                                                                                      | Default                        |
|:-------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------|
| `ranger.auth-type` | Authentication type to be used. Allowed values are `simple \| kerberos`                                                                                                                                                                          | `simple`                       |
| `ranger.username`  | Username to be used on authentication. On `kerberos` authentication, it corresponds to the principal. Override if it's necessary to define different credentials than the ones defined on `CDP_DEPLOY_ROLE_USER` env variable.                   | `${?CDP_DEPLOY_ROLE_USER}`     |
| `ranger.password`  | Password to be used on authentication. On `kerberos` authentication, it corresponds to the path to the keytab file. Override if it's necessary to define different credentials than the ones defined on `CDP_DEPLOY_ROLE_PASSWORD` env variable. | `${?CDP_DEPLOY_ROLE_PASSWORD}` |
| `ranger.base-url`  | Base URL in the form `http[s]://<HOST>:<PORT>/` to contact Ranger. Required only when `provision-cloud = private`.                                                                                                                               |                                |

Example:

```
ranger {
    auth-type = "kerberos"
    base-url = "https://ranger.endpoint/ranger/"
    username = "admin"
    password = "path/to/file.keytab"
}
```

We recommend storing password on environment variables and referencing them with the `${?<env_name>}` syntax rather than writing them in your `application.conf`.