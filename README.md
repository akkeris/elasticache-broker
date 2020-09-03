# Akkeris Redis and Memcached (ElastiCache) Broker

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/e254f42669d24067b453298da2297e2d)](https://www.codacy.com/gh/akkeris/elasticache-broker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=akkeris/elasticache-broker&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/e254f42669d24067b453298da2297e2d)](https://www.codacy.com/gh/akkeris/elasticache-broker?utm_source=github.com&utm_medium=referral&utm_content=akkeris/elasticache-broker&utm_campaign=Badge_Coverage)

[![CircleCI](https://circleci.com/gh/akkeris/elasticache-broker.svg?style=svg)](https://circleci.com/gh/akkeris/elasticache-broker)

The ElastiCache broker is an Open Service Broker (OSB) cache broker that will provision redis and memcached instances on demand through a REST API. While it only supports AWS at the moment, support for more providers is on its way.

## Providers

The broker has support for the following providers

* AWS Memcached
* AWS Redis

## Features

* Create your own plans
* Upgrade plans
* Restart
* Preprovisioning memcached and redis instances for speed

## Installing

1. Create a postgres database
2. Deploy the docker image (both worker and api) `akkeris/elasticache-broker:latest` run `start.sh` for the api, `start-backgrouund.sh` for worker. Both should use the same settigs (env) below.

### 1. Settings

Note almost all of these can be set via the command line as well.

**Required**

* `DATABASE_URL` - The postgres database to store its information on what databases its provisioned, this should be in the format of `postgres://user:password@host:port/database?sslmode=disable` or leave off sslmode=disable if ssl is supported.  This will auto create the schema if its unavailable.
* `NAME_PREFIX` - The prefix to use for all provisioned databases this should be short and help namespace databases created by the broker vs. other databases that may exist in the broker for other purposes. This is global to all of the providers configured.

**AWS Provider Specific**

* `AWS_REGION` - The AWS region to provision databases in, only one aws provider and region are supported by the database broker.
* `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` to an IAM role that has full access to RDS in the `AWS_REGION` you specified above.
* `ELASTICACHE_SECURITY_GROUP` The security group in AWS for elasticache instances.
* `REDIS_SUBNET_GROUP` is the subnet group for redis instances.
* `MEMCACHED_SUBNET_GROUP` is the subnet group for memcached instances.

Note that you can get away with not setting `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and use EC2 IAM roles or hard coded credentials via the `~/.aws/credentials` file but these are not recommended!

**Kubernetes Provider Specfic**

* `USE_KUBERNETES` - must be set to `true` to use kubernetes
* `USE_LOCAL_KUBE_CONTEXT` - By default only a in-cluster service account will be auto detected, to allow elasticache broker to use your `~/.kube/config` with the local context this must be set to `true`. Use `kubectl config use-context [xyz]` to set the context it should use locally. This should only be set if you are developing locally.

In addition, the namespace `memcached-system` must be created with a service account to create, update and delete servces/deployments/pods in this namespace attached to the deployment of the elasticache broker.

**Optional**

* `PORT` - This defaults to 8443, setting this changes the default port number to listen to http (or https) traffic on
* `RETRY_WEBHOOKS` - (WORKER ONLY) whether outbound notifications about provisions or create bindings should be retried if they fail.  This by default is false, unless you trust or know the clients hitting this broker, leave this disabled.

### 2. Deployment

You can deploy the image `akkeris/elasticache-broker:latest` via docker with the environment or config var settings above. If you decide you're going to build this manually and run it you'll need see the Building section below. 

### 3. Plans

Plans can be created by modifying the database table called "plans". They provide a great way of limiting the scope, capability and offerings to whomever is using the broker. See [docs/PLANS.md](plans) for more information. By default the elasticache-broker will initially load with plans for aws and shared postgres. 

### 4. Setup Task Worker

You'll need to deploy one or multiple (depending on your load) task workers with the same config or settings specified in Step 1. but with a different startup command, append the `-background-tasks` option to the service brokers startup command to put it into worker mode.  You MUST have at least 1 worker.

## Running

As described in the setup instructions you should have two deployments for your application, the first is the API that receives requests, the other is the tasks process.  See `start.sh` for the API startup command, see `start-background.sh` for the tasks process startup command. Both of these need the above environment variables in order to run correctly.

**Debugging**

You can optionally pass in the startup options `-logtostderr=1 -stderrthreshold 0` to enable debugging, in addition you can set `GLOG_logtostderr=1` to debug via the environment.  See glog for more information on enabling various levels. You can also set `STACKIMPACT` as an environment variable to have profiling information sent to stack impact. 

## Contributing and Building

1. `export GO111MODULE=on`
2. `make`
3. `./servicebroker ...`

### Testing

Working on it...


