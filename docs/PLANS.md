## Plans

Plans are modified by changing the data in database (specifically the plans table) directly.  See `pkg/broker/storage.go` for pre-populated set of "default" plans.

TODO: Describe how plans work a bit more thoroughly.

### AWS ElastiCache Settings

```

{
	"AZMode":null,
	"AuthToken":null,
	"AutoMinorVersionUpgrade":true,
	"CacheClusterId":null,
	"CacheNodeType":"cache.t2.micro",
	"CacheParameterGroupName":"default.redis5.0",
	"CacheSecurityGroupNames":null,
	"CacheSubnetGroupName":"${REDIS_SUBNET_GROUP}",
	"Engine":"redis",
	"EngineVersion":"5.0.4",
	"NotificationTopicArn":null,
	"NumCacheNodes":1,
	"Port":6379,
	"PreferredAvailabilityZone":null,
	"PreferredAvailabilityZones":null,
	"PreferredMaintenanceWindow":null,
	"ReplicationGroupId":null,
	"SecurityGroupIds":["${ELASTICACHE_SECURITY_GROUP}"],
	"SnapshotArns":null,
	"SnapshotName":null,
	"SnapshotRetentionLimit":null,
	"SnapshotWindow":null,
	"Tags":null
}
```

