# cdp-impala-provisioner

![Version: 0.1.9](https://img.shields.io/badge/Version-0.1.9-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.16.0](https://img.shields.io/badge/AppVersion-1.16.0-informational?style=flat-square)

A Helm chart for Kubernetes

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| cdpPrivate | object | `{"enabled":false}` | This configuration allows to toggle between CDP public or private  |
| configOverride | string | `nil` | This configuration allows you to override the application.conf file |
| customRootCA.enabled | bool | `true` | If this option is enabled, the chart will load the custom CA from a secret with key `cdp-private-impala-custom-ca`. The CA is expected to be in a format compatible with `keytool` utility (PEM works fine). |
| dockerRegistrySecretName | string | `"regcred"` | Docker Registry Secret name used to access a private repo |
| existingServiceAccount | string | `"default"` | the name of an existing serviceAccount |
| image.pullPolicy | string | `"Always"` | The imagePullPolicy for a container and the tag of the image affect when the kubelet attempts to pull (download) the specified image. |
| image.registry | string | `"registry.gitlab.com/agilefactory/witboost.mesh/provisioning/cdp-refresh/witboost.mesh.provisioning.outputport.cdp.impala"` | Image repository |
| image.tag | string | `"latest"` | Image tag |
| kerberos.enabled | bool | `false` | Enables Kerberos configuration injection |
| kerberos.jaasOverride | string | `nil` | This configuration allows you to override the jaas.conf file |
| kerberos.krb5Override | string | `nil` | This configuration allows you to override the krb5.conf file |
| livenessProbe | object | `{"httpGet":{"path":"/health","port":8093}}` | liveliness probe spec |
| logbackOverride | string | `nil` | This configuration allows you to override the logback.xml file |
| readinessProbe | object | `{}` | readiness probe spec |
| replicas | int | `1` | the number of pod replicas |
| resources | object | `{}` | resources spec |
| securityContext | object | `{"allowPrivilegeEscalation":false,"runAsNonRoot":false,"runAsUser":1001}` | security context spec |
| serviceAccount | object | `{"create":false,"roleArn":null}` | service account nme |
| serviceAccount.roleArn | string | `nil` | The AWS role arn that will be assumed |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.11.0](https://github.com/norwoodj/helm-docs/releases/v1.11.0)
