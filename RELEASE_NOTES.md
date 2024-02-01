# Changelog

All notable changes to this project will be documented in this file.

## v1.0.0

### Commits

- **[WIT-1225] Refactor Impala provisioner to allow implementing private flow with minimal code**
  > 
  > ##### New features and improvements
  > 
  > * Refactored provisioner to remove dependencies inherent to public cloud on some classes
  > * Generalized the specific schema of the output port component to allow for future different specific schemas.
  > * Migrated provisioner to work with new version of scala-mesh-commons library (WIT-1224), specifically the Ranger endpoint handling
  > 
  > ##### Related issue
  > 
  > Closes WIT-1225
  > 
  > 

- **[WIT-970] Implement Update ACL support in Impala SP**
  > 
  > ##### New features and improvements
  > 
  > List any new features and improvements made here.
  > 
  > ##### Breaking changes
  > 
  > List any breaking changes here, along with a brief reason why they were made (eg to add a new feature).
  > 
  > ##### Migration
  > 
  > If any breaking changes were made, list how to handle them here.
  > 
  > ##### Bug fixes
  > 
  > List any bug fixes made here.
  > 
  > ##### Related issue
  > 
  > Closes WIT-970
  > 
  > 

- **[WIT-945] Impala SP fails to find a folder if it wasnt created manually**
  > 
  > ##### New features and improvements
  > 
  > * Added correct validation on existence S3 object/folder
  > 
  > ##### Bug fixes
  > 
  > * Fixes a bug where the validation of the existence of an S3 folder would fail if that bucket wasn't created manually (a.k.a. there exists an [empty] object with the name of the expected folder)
  > 
  > ##### Related issue
  > 
  > Closes WIT-945
  > 
  > 

- **[WIT-692] CDP Private Impala SP HLD**
  > 
  > ##### New features and improvements
  > 
  > Adds the HLD for CDP Private for Impala SP
  > 
  > ##### Related issue
  > 
  > Closes WIT-962
  > 
  > 

- **[WIT-802] Helm chart for CDP Impala SP**
  > 
  > ##### New features and improvements
  > 
  > This MR adds the helm chart to deploy the cdp impala provisioner on k8s
  > 
  > ##### 
  > 
  > 

- **[WIT-812] HLD for CDP Impala SP**
  > 
  > ##### New features and improvements
  > 
  > Added HLD diagrams and its documentation
  > 
  > ##### Related issue
  > 
  > Closes WIT-812
  > 
  > 

- **[WIT-876] Make Impala SP drop the db and table on unprovision**
  > 
  > ##### New features and improvements
  > 
  > * Added functionality to drop the created tables when performing unprovisioning. This is configurable through a configuration file
  > 
  > ##### Breaking changes
  > 
  > * Tables are now dropped by default when unprovisioning a component
  > 
  > ##### Migration
  > 
  > * If the provisioner wants to keep the old functionality of not dropping tables, set the configuration `drop-on-unprovision` to `false`.
  > 
  > ##### Related issue
  > 
  > Closes WIT-876
  > 
  > 

- **[WIT-863] Fixed Ranger SZ generator and RoleConfigRepository**
  > 
  > ##### New features and improvements
  > 
  > - Filters out Ranger Security Zone resources if its corresponding list is empty
  > 
  > ##### Bug fixes
  > 
  > - Fixes a bug where role mapping would fail if the ref included special characters, creating policies without assigning users to the policy
  > - Fixes the unprovision operation where the Ranger Security Zone would fail to update
  > 
  > ##### Related issue
  > 
  > Closes WIT-863
  > 
  > 

- **[WIT-367] CDP Specific Provisioner for Impala cleanup and refactoring**
  > 
  > ##### New features and improvements
  > 
  > Cleanup and refactoring
  > 
  > ##### Related issue
  > 
  > Closes WIT-367
  > 
  > 

- **Update file README.md**



