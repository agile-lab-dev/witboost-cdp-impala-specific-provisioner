# Configuring the Impala Specific Provisioner

Most application configurations are handled with the Typesafe Config library. You can find the default settings in the `reference.conf` of each module. Customize them and use the `config.file` system property or the other options provided by Typesafe Config according to your needs. The provided docker image expects the config file mounted at path `/config/application.conf`.

## Provisioner configuration

| Configuration                                 | Description                                                                                              | Default   |
|:----------------------------------------------|:---------------------------------------------------------------------------------------------------------|:----------|
| `provisioner.networking.httpServer.interface` | Interface to bind the specific provisioner API layer                                                     | `0.0.0.0` |
| `provisioner.networking.httpServer.port`      | Port to bind the specific provisioner API layer                                                          | `8080`    |
| `provisioner.provision-cloud`                 | Type of CDP environment that sets the tasks done by the provisioner                                      | `public`  |
| `provisioner.cluster-name`                    | Cluster name of the CDP environment to be used. Required only when `provisioner.provision-cloud=private` |           |

Example:

```
provisioner {
  networking {
    httpServer {
      port: 8080
      interface: "0.0.0.0"
    }
  }
  
  provision-cloud = public
  cluster-name = ""
}
```


## Service configuration

| Configuration                                | Description                                                                                                                       | Default   |
|:---------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------|:----------|
| `impala.port`                                | Port to be used to establish JDBC connections to Impala                                                                           | `443`     |
| `impala.schema`                              | Schema to be used on the JDBC connection string to Impala                                                                         | `default` |
| `impala.drop-on-unprovision`                 | Drops the created external tables at unprovision time                                                                             | `true`    |
| `impala.principalsMappingPlugin`             | Object containing the configuration for instantiating the PrincipalsMapper                                                        |           |  
| `impala.principalsMappingPlugin.pluginClass` | Fully qualified name of the PrincipalsMapperFactory to be instantiated                                                            |           |
| `impala.private-cloud.coordinator-host-urls` | List of Impala coordinator hosts available to receive JDBC connections. Only required if `provisioner.provision-cloud = private`. | `[]`      |

Each PrincipalsMapperFactory may require additional configuration information to create the PrincipalsMapper instance. This configuration is retrieved from an object in the `principalsMappingPlugin` config object. The key of this object shall be equal to the `configIdentifier` of the instantiated PrincipalsMapperFactory. **Even if the PrincipalsMapper doesn't require additional configuration, the key must be present as an empty object**. 

### FreeIpaIdentityPrincipalsMapper

PrincipalsMapper to query the FreeIPA instance of a CDP Public Cloud leveraging the Cloudera SDK.

Example of configuration to load `FreeIpaIdentityPrincipalsMapper`:

```
impala {
  ...
  principalsMappingPlugin {
    pluginClass = "it.agilelab.provisioning.commons.principalsmapping.impl.freeipa.FreeIpaIdentityPrincipalsMapperFactory"
    freeipa-identity {}
  }
}
```

### LdapPrincipalsMapper

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
impala {
  ...
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
}
```

## HDFS configuration

Configuration used only on CDP Private environments (`provision-cloud = private`). Left empty if CDP Public with S3 is being used.

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

Ranger configuration is used to define the authentication method to access the platform. On CDP Private Cloud environment it also requires to define the base URL to access, while on CDP Public Cloud this is automatically discovered. This configuration gives the possibility to define different credentials for deploying and accessing Ranger by overriding the username and password fields.

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