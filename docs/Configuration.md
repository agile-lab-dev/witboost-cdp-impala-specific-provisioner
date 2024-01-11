# Configuring the Impala Specific Provisioner

Most application configurations are handled with the Typesafe Config library. You can find the default settings in the `reference.conf` of each module. Customize them and use the `config.file` system property or the other options provided by Typesafe Config according to your needs. The provided docker image expects the config file mounted at path `/config/application.conf`.

## Provisioner configuration

| Configuration                               | Description                                          | Default   |
|:--------------------------------------------|:-----------------------------------------------------|:----------|
| provisioner.networking.httpServer.interface | Interface to bind the specific provisioner API layer | `0.0.0.0` |
| provisioner.networking.httpServer.port      | Port to bind the specific provisioner API layer      | `8080`    |

Example:

```
provisioner {
  networking {
    httpServer {
      port: 8080
      interface: "0.0.0.0"
    }
  }
}
```


## Service configuration

| Configuration                       | Description                                                                | Default |
|:------------------------------------|:---------------------------------------------------------------------------|:--------|
| drop-on-unprovision                 | Drops the created external tables at unprovision time                      | `true`  |
| principalsMappingPlugin             | Object containing the configuration for instantiating the PrincipalsMapper |         |  
| principalsMappingPlugin.pluginClass | Fully qualified name of the PrincipalsMapperFactory to be instantiated     |         |

Each PrincipalsMapperFactory may require additional configuration information to create the PrincipalsMapper instance. This configuration is retrieved from an object in the `principalsMappingPlugin` config object. The key of this object shall be equal to the `configIdentifier` of the instantiated PrincipalsMapperFactory

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

