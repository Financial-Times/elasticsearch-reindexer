# UPP Concepts ElasticSearch Reindexer

Checks the existing alias for the concepts ElasticSearch index and if necessary, creates a new mapping, reindexes the data, and adjusts the alias to point to the new index.

## Code

concepts-reindexer

## Primary URL

<https://upp-prod-delivery-glb.upp.ft.com/__concept-search-reindexer/>

## Service Tier

Bronze

## Lifecycle Stage

Production

## Delivered By

content

## Supported By

content

## Known About By

- elitsa.pavlova
- kalin.arsov
- miroslav.gatsanoga
- ivan.nikolov
- marina.chompalova
- hristo.georgiev
- mihail.mihaylov

## Host Platform

AWS

## Architecture

This is a service that can be used to migrate data from an existing index to a new index with updated mappings. The service assumes that the index is behind an alias, and that the current index can be made read-only while the data is copied from the current index into a new version of that index. Once the new index has been written, the alias is cut over to the new version.

## Contains Personal Data

No

## Contains Sensitive Data

No

## Dependencies

- concept-elasticsearch-cluster

## Failover Architecture Type

ActiveActive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The service is deployed in both Delivery clusters. The failover guide for the cluster is located here:
<https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/delivery-cluster>

## Data Recovery Process Type

NotApplicable

## Data Recovery Details

The service does not store data, so it does not require any data recovery steps.

## Release Process Type

PartiallyAutomated

## Rollback Process Type

Manual

## Release Details

The release is triggered by making a Github release in the [concept-search-index-mapping](https://github.com/Financial-Times/concept-search-index-mapping) repository which is then picked up by a Jenkins multibranch pipeline. The Jenkins pipeline should be manually started in order for it to deploy the helm package to the Kubernetes clusters. The deployment is configured to be with zero replicas, scaling it up will trigger the ES reindexing process. If reindexing is done in Production, it should be done one region at a time to avoid blocking the whole concept publishing flow.

## Key Management Process Type

Manual

## Key Management Details

To access the service clients need to provide basic auth credentials.
To rotate credentials you need to login to a particular cluster and update varnish-auth secrets.

## Monitoring

Service in UPP K8S delivery clusters:

- Delivery-Prod-EU health: <https://upp-prod-delivery-eu.upp.ft.com/__health/__pods-health?service-name=concept-search-reindexer>
- Delivery-Prod-US health: <https://upp-prod-delivery-us.upp.ft.com/__health/__pods-health?service-name=concept-search-reindexer>

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.
