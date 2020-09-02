package broker

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/go-redis/redis"
	"github.com/golang/glog"
	"os"
	"strconv"
	"strings"
	"time"
)

type AWSInstanceRedisProvider struct {
	Provider
	awssvc        *elasticache.ElastiCache
	namePrefix    string
	instanceCache map[string]*Instance
}

func NewAWSInstanceRedisProvider(namePrefix string) (*AWSInstanceRedisProvider, error) {
	if os.Getenv("AWS_REGION") == "" {
		return nil, errors.New("Unable to find AWS_REGION environment variable.")
	}
	t := time.NewTicker(time.Second * 5)
	AWSInstanceRedisProvider := &AWSInstanceRedisProvider{
		namePrefix:    namePrefix,
		instanceCache: make(map[string]*Instance),
		awssvc:        elasticache.New(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})),
	}
	go (func() {
		for {
			AWSInstanceRedisProvider.instanceCache = make(map[string]*Instance)
			<-t.C
		}
	})()
	return AWSInstanceRedisProvider, nil
}

func (provider AWSInstanceRedisProvider) GetInstance(name string, plan *ProviderPlan) (*Instance, error) {
	if provider.instanceCache[name+plan.ID] != nil {
		return provider.instanceCache[name+plan.ID], nil
	}
	resp, err := provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId:    aws.String(name),
		MaxRecords:        aws.Int64(20),
		ShowCacheNodeInfo: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	var endpoint = ""
	if len(resp.CacheClusters) > 0 && len(resp.CacheClusters[0].CacheNodes) > 0 && resp.CacheClusters[0].CacheNodes[0].Endpoint != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Port != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Address != nil {
		endpoint = *resp.CacheClusters[0].CacheNodes[0].Endpoint.Address + ":" + strconv.FormatInt(*resp.CacheClusters[0].CacheNodes[0].Endpoint.Port, 10)
	}
	provider.instanceCache[name+plan.ID] = &Instance{
		Id:            "", // providers should not store this.
		ProviderId:    *resp.CacheClusters[0].CacheClusterId,
		Name:          name,
		Plan:          plan,
		Username:      "", // providers should not store this.
		Password:      "", // providers should not store this.
		Endpoint:      endpoint,
		Status:        *resp.CacheClusters[0].CacheClusterStatus,
		Ready:         IsReady(*resp.CacheClusters[0].CacheClusterStatus),
		Engine:        *resp.CacheClusters[0].Engine,
		EngineVersion: *resp.CacheClusters[0].EngineVersion,
		Scheme:        plan.Scheme,
	}

	return provider.instanceCache[name+plan.ID], nil
}

func (provider AWSInstanceRedisProvider) PerformPostProvision(db *Instance) (*Instance, error) {
	return db, nil
}

func (provider AWSInstanceRedisProvider) GetUrl(instance *Instance) map[string]interface{} {
	return map[string]interface{}{
		"REDIS_URL": instance.Scheme + "://" + instance.Endpoint,
	}
}

func (provider AWSInstanceRedisProvider) ProvisionWithSettings(Id string, plan *ProviderPlan, settings *elasticache.CreateCacheClusterInput) (*Instance, error) {
	// TODO: Support CreateReplicationGroup rather than a single cache cluster.
	resp, err := provider.awssvc.CreateCacheCluster(settings)
	if err != nil {
		return nil, err
	}

	var endpoint = ""
	if len(resp.CacheCluster.CacheNodes) > 0 && resp.CacheCluster.CacheNodes[0].Endpoint != nil && resp.CacheCluster.CacheNodes[0].Endpoint.Port != nil && resp.CacheCluster.CacheNodes[0].Endpoint.Address != nil {
		endpoint = *resp.CacheCluster.CacheNodes[0].Endpoint.Address + ":" + strconv.FormatInt(*resp.CacheCluster.CacheNodes[0].Endpoint.Port, 10)
	}

	return &Instance{
		Id:            Id,
		Name:          *resp.CacheCluster.CacheClusterId,
		ProviderId:    *resp.CacheCluster.CacheClusterId,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      endpoint,
		Status:        *resp.CacheCluster.CacheClusterStatus,
		Ready:         IsReady(*resp.CacheCluster.CacheClusterStatus),
		Engine:        *resp.CacheCluster.Engine,
		EngineVersion: *resp.CacheCluster.EngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider AWSInstanceRedisProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*Instance, error) {
	var settings elasticache.CreateCacheClusterInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	settings.CacheClusterId = aws.String(strings.ToLower(provider.namePrefix + RandomString(8)))
	settings.Tags = []*elasticache.Tag{{Key: aws.String("BillingCode"), Value: aws.String(Owner)}}
	return provider.ProvisionWithSettings(Id, plan, &settings)
}

func (provider AWSInstanceRedisProvider) Deprovision(Instance *Instance, takeSnapshot bool) error {
	var snapshot *string = nil
	if takeSnapshot {
		snapshot = aws.String(Instance.ProviderId + "-final")
	}
	_, err := provider.awssvc.DeleteCacheCluster(&elasticache.DeleteCacheClusterInput{
		CacheClusterId:          aws.String(Instance.ProviderId),
		FinalSnapshotIdentifier: snapshot,
	})
	return err
}

func (provider AWSInstanceRedisProvider) ModifyWithSettings(instance *Instance, plan *ProviderPlan, settings *elasticache.CreateCacheClusterInput) (*Instance, error) {
	glog.Infof("Instance: %s modifying settings...\n", instance.Id)
	// TODO: Support ModifyReplicationGroup rather than a single cache cluster.
	resp, err := provider.awssvc.ModifyCacheCluster(&elasticache.ModifyCacheClusterInput{
		AZMode:                     settings.AZMode,
		ApplyImmediately:           aws.Bool(true),
		AutoMinorVersionUpgrade:    settings.AutoMinorVersionUpgrade,
		CacheClusterId:             aws.String(instance.ProviderId),
		CacheNodeType:              settings.CacheNodeType,
		CacheParameterGroupName:    settings.CacheParameterGroupName,
		CacheSecurityGroupNames:    settings.CacheSecurityGroupNames,
		EngineVersion:              settings.EngineVersion,
		NotificationTopicArn:       settings.NotificationTopicArn,
		NumCacheNodes:              settings.NumCacheNodes,
		PreferredMaintenanceWindow: settings.PreferredMaintenanceWindow,
		SecurityGroupIds:           settings.SecurityGroupIds,
		SnapshotRetentionLimit:     settings.SnapshotRetentionLimit,
		SnapshotWindow:             settings.SnapshotWindow,
	})
	if err != nil {
		return nil, err
	}

	tick := time.NewTicker(time.Second * 30)
	<-tick.C

	var endpoint = ""
	if resp.CacheCluster.CacheNodes[0].Endpoint != nil && resp.CacheCluster.CacheNodes[0].Endpoint.Port != nil && resp.CacheCluster.CacheNodes[0].Endpoint.Address != nil {
		endpoint = *resp.CacheCluster.CacheNodes[0].Endpoint.Address + ":" + strconv.FormatInt(*resp.CacheCluster.CacheNodes[0].Endpoint.Port, 10)
	}

	res, err := provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId:    aws.String(instance.ProviderId),
		MaxRecords:        aws.Int64(20),
		ShowCacheNodeInfo: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(res.CacheClusters) < 1 || len(res.CacheClusters[0].CacheNodes) < 1 {
		return nil, errors.New("No cluster or nodes were found in instance after modification!")
	}
	glog.Infof("Instance: %s modifications finished.\n", instance.Id)
	return &Instance{
		Id:            instance.Id,
		Name:          instance.Name,
		ProviderId:    *res.CacheClusters[0].CacheClusterId,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      endpoint,
		Status:        *res.CacheClusters[0].CacheClusterStatus,
		Ready:         IsReady(*res.CacheClusters[0].CacheClusterStatus),
		Engine:        *res.CacheClusters[0].Engine,
		EngineVersion: *res.CacheClusters[0].EngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider AWSInstanceRedisProvider) Modify(Instance *Instance, plan *ProviderPlan) (*Instance, error) {
	if !CanBeModified(Instance.Status) {
		return nil, errors.New("Databases cannot be modifed during backups, upgrades or while maintenance is being performed.")
	}
	var settings elasticache.CreateCacheClusterInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	return provider.ModifyWithSettings(Instance, plan, &settings)
}

func (provider AWSInstanceRedisProvider) Tag(Instance *Instance, Name string, Value string) error {
	_, err := provider.awssvc.AddTagsToResource(&elasticache.AddTagsToResourceInput{
		ResourceName: aws.String(Instance.ProviderId),
		Tags: []*elasticache.Tag{
			{
				Key:   aws.String(Name),
				Value: aws.String(Value),
			},
		},
	})
	return err
}

func (provider AWSInstanceRedisProvider) Untag(Instance *Instance, Name string) error {
	_, err := provider.awssvc.RemoveTagsFromResource(&elasticache.RemoveTagsFromResourceInput{
		ResourceName: aws.String(Instance.ProviderId),
		TagKeys: []*string{
			aws.String(Name),
		},
	})
	return err
}

func (provider AWSInstanceRedisProvider) Restart(Instance *Instance) error {
	if !Instance.Ready {
		return errors.New("Cannot restart a database that is unavailable.")
	}
	res, err := provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId:    aws.String(Instance.ProviderId),
		MaxRecords:        aws.Int64(20),
		ShowCacheNodeInfo: aws.Bool(true),
	})
	if err != nil {
		return err
	}
	if len(res.CacheClusters) < 1 || len(res.CacheClusters[0].CacheNodes) < 1 {
		return errors.New("No cluster or nodes were found to reboot!")
	}
	var nodes []*string
	for _, node := range res.CacheClusters[0].CacheNodes {
		nodes = append(nodes, node.CacheNodeId)
	}
	_, err = provider.awssvc.RebootCacheCluster(&elasticache.RebootCacheClusterInput{
		CacheClusterId:       aws.String(Instance.ProviderId),
		CacheNodeIdsToReboot: nodes,
	})
	return err
}

func (provider AWSInstanceRedisProvider) Flush(Instance *Instance) error {
	return errors.New("Flush is not available on redis instances.")
}

func (provider AWSInstanceRedisProvider) Stats(Instance *Instance) ([]Stat, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     Instance.Endpoint,
		Password: Instance.Password,
		DB:       0,
	})
	defer client.Close()
	info, err := client.Info().Result()
	if err != nil {
		return nil, err
	}
	infos := strings.Split(info, "\n")
	stats := make([]Stat, 0)

	for _, keyValLine := range infos {
		if strings.TrimSpace(keyValLine) != "" && len(keyValLine) > 0 && keyValLine[0] != '#' {
			sep := strings.Split(keyValLine, ":")
			if len(sep) == 2 {
				stats = append(stats, Stat{
					Key:   sep[0],
					Value: strings.Trim(strings.TrimSpace(sep[1]), "\r"),
				})
			}
		}
	}

	return stats, nil
}

func (provider AWSInstanceRedisProvider) GetBackup(instance *Instance, Id string) (*BackupSpec, error) {
	snapshots, err := provider.awssvc.DescribeSnapshots(&elasticache.DescribeSnapshotsInput{
		CacheClusterId: aws.String(instance.Name),
		SnapshotName:   aws.String(Id),
	})
	if err != nil {
		return nil, err
	}
	if len(snapshots.Snapshots) != 1 {
		return nil, errors.New("No backups were found.")
	}
	if len(snapshots.Snapshots[0].NodeSnapshots) == 0 {
		return nil, errors.New("No data for any nodes was found in the backups.")
	}

	created := time.Now().UTC().Format(time.RFC3339)
	if snapshots.Snapshots[0].NodeSnapshots[0].SnapshotCreateTime != nil {
		created = snapshots.Snapshots[0].NodeSnapshots[0].SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	var progress int64 = 100
	if *snapshots.Snapshots[0].SnapshotStatus == "creating" {
		progress = 50
	}

	return &BackupSpec{
		Resource: ResourceSpec{
			Name: instance.Name,
		},
		Id:       snapshots.Snapshots[0].SnapshotName,
		Progress: aws.Int64(progress),
		Status:   snapshots.Snapshots[0].SnapshotStatus,
		Created:  created,
	}, nil
}

func (provider AWSInstanceRedisProvider) ListBackups(instance *Instance) ([]BackupSpec, error) {
	snapshots, err := provider.awssvc.DescribeSnapshots(&elasticache.DescribeSnapshotsInput{CacheClusterId: aws.String(instance.Name)})
	if err != nil {
		return []BackupSpec{}, err
	}
	out := make([]BackupSpec, 0)
	for _, snapshot := range snapshots.Snapshots {
		if len(snapshot.NodeSnapshots) > 0 {
			created := time.Now().UTC().Format(time.RFC3339)
			if snapshot.NodeSnapshots[0].SnapshotCreateTime != nil {
				created = snapshot.NodeSnapshots[0].SnapshotCreateTime.UTC().Format(time.RFC3339)
			}
			var progress int64 = 100
			if *snapshot.SnapshotStatus == "creating" {
				progress = 50
			}
			out = append(out, BackupSpec{
				Resource: ResourceSpec{
					Name: instance.Name,
				},
				Id:       snapshot.SnapshotName,
				Progress: aws.Int64(progress),
				Status:   snapshot.SnapshotStatus,
				Created:  created,
			})
		}
	}
	return out, nil
}

func (provider AWSInstanceRedisProvider) CreateBackup(instance *Instance) (*BackupSpec, error) {
	if !instance.Ready {
		return nil, errors.New("Cannot create read only user on database that is unavailable.")
	}
	snapshotOut, err := provider.awssvc.CreateSnapshot(&elasticache.CreateSnapshotInput{
		CacheClusterId: aws.String(instance.Name),
		SnapshotName:   aws.String(instance.Name + "-manual-" + RandomString(10)),
	})
	if err != nil {
		return nil, err
	}
	snapshot := snapshotOut.Snapshot

	if len(snapshot.NodeSnapshots) == 0 {
		return nil, errors.New("No data for any nodes was found in the backup.")
	}

	created := time.Now().UTC().Format(time.RFC3339)
	if snapshot.NodeSnapshots[0].SnapshotCreateTime != nil {
		created = snapshot.NodeSnapshots[0].SnapshotCreateTime.UTC().Format(time.RFC3339)
	}
	var progress int64 = 100
	if *snapshot.SnapshotStatus == "creating" {
		progress = 50
	}

	return &BackupSpec{
		Resource: ResourceSpec{
			Name: instance.Name,
		},
		Id:       snapshot.SnapshotName,
		Progress: aws.Int64(progress),
		Status:   snapshot.SnapshotStatus,
		Created:  created,
	}, nil
}

func (provider AWSInstanceRedisProvider) RestoreBackup(instance *Instance, Id string) error {
	var settings elasticache.CreateCacheClusterInput
	if err := json.Unmarshal([]byte(instance.Plan.providerPrivateDetails), &settings); err != nil {
		return err
	}

	// Validate restore backup
	backup, err := provider.GetBackup(instance, Id)
	if err != nil {
		return errors.New("Unable to restore backup, as the backup could not be found.")
	}

	if !instance.Ready {
		return errors.New("Cannot restore a backup on this redis because redis is unavailable.")
	}

	if *backup.Status != "available" {
		return errors.New("Cannot restore a backup that is not available to be used.")
	}

	// For AWS, the best strategy for restoring (reliably) a redis is to rename the existing db
	// then create from a snapshot the existing db, and then nuke the old one once finished.
	awsResp, err := provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: aws.String(instance.Name),
		MaxRecords:     aws.Int64(20),
	})
	if err != nil {
		return err
	}
	if len(awsResp.CacheClusters) != 1 {
		return errors.New("Unable to find database to rebuild as none or multiple were returned")
	}

	// tagsResp, err := provider.awssvc.ListTagsForResource(&elasticache.ListTagsForResourceInput{
	// 	ResourceName:aws.String(instance.Name),
	// })
	// if err != nil {
	// 	glog.Errorf("ERROR: Cannot pull tags for %s: %s\n", instance.Id, err.Error())
	// 	return err
	// }

	// Start Restore Process
	var privateSecurityGroups []*string = make([]*string, 0)
	for _, group := range awsResp.CacheClusters[0].SecurityGroups {
		privateSecurityGroups = append(privateSecurityGroups, group.SecurityGroupId)
	}

	var publicSecurityGroups []*string = make([]*string, 0)
	for _, group := range awsResp.CacheClusters[0].CacheSecurityGroups {
		publicSecurityGroups = append(publicSecurityGroups, group.CacheSecurityGroupName)
	}

	renamedId := instance.Name + "-restore-" + RandomString(5)

	_, err = provider.awssvc.DeleteCacheCluster(&elasticache.DeleteCacheClusterInput{
		CacheClusterId:          aws.String(instance.Name),
		FinalSnapshotIdentifier: aws.String(renamedId),
	})
	if err != nil {
		glog.Errorf("ERROR: Removing the existing cache cluster failed!: %s %s\n", renamedId, err.Error())
		return err
	}

	err = provider.awssvc.WaitUntilCacheClusterDeleted(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: aws.String(instance.Name),
	})
	if err != nil {
		glog.Errorf("ERROR: Timeout or error waiting for resource to be deleted: %s %s\n", instance.Id, err.Error())
		return err
	}

	var notificationTopicArn *string = nil
	if awsResp.CacheClusters[0].NotificationConfiguration != nil {
		notificationTopicArn = awsResp.CacheClusters[0].NotificationConfiguration.TopicArn
	}
	var cacheParameterGroupName *string = nil
	if awsResp.CacheClusters[0].CacheParameterGroup != nil {
		cacheParameterGroupName = awsResp.CacheClusters[0].CacheParameterGroup.CacheParameterGroupName
	}
	var authToken *string = nil
	if awsResp.CacheClusters[0].AuthTokenEnabled != nil && *awsResp.CacheClusters[0].AuthTokenEnabled == true {
		authToken = aws.String(instance.Password)
	}
	var port *int64 = nil
	if awsResp.CacheClusters[0].ConfigurationEndpoint != nil {
		port = awsResp.CacheClusters[0].ConfigurationEndpoint.Port
	}

	// TODO: Support CreateReplicationGroup rather than a single cache cluster.

	_, err = provider.awssvc.CreateCacheCluster(&elasticache.CreateCacheClusterInput{
		// -- AZMode - intentionally left out as it only applies to memcached.
		AuthToken:                 authToken,
		AutoMinorVersionUpgrade:   awsResp.CacheClusters[0].AutoMinorVersionUpgrade,
		CacheClusterId:            aws.String(instance.Name),
		CacheNodeType:             awsResp.CacheClusters[0].CacheNodeType,
		CacheParameterGroupName:   cacheParameterGroupName,
		CacheSecurityGroupNames:   publicSecurityGroups,                          // only on non-VPC systems
		CacheSubnetGroupName:      awsResp.CacheClusters[0].CacheSubnetGroupName, // only on VPC systems
		Engine:                    awsResp.CacheClusters[0].Engine,
		EngineVersion:             awsResp.CacheClusters[0].EngineVersion,
		NotificationTopicArn:      notificationTopicArn,
		NumCacheNodes:             awsResp.CacheClusters[0].NumCacheNodes,
		Port:                      port,
		PreferredAvailabilityZone: awsResp.CacheClusters[0].PreferredAvailabilityZone,
		// -- PreferredAvailabilityZones - Intentionally left out as it only applies to memcached.
		PreferredMaintenanceWindow: awsResp.CacheClusters[0].PreferredMaintenanceWindow,
		ReplicationGroupId:         awsResp.CacheClusters[0].ReplicationGroupId,
		SecurityGroupIds:           privateSecurityGroups,
		// -- SnapshotArns -- intentionally left out as I believe it will try to restore from this.
		SnapshotName:           aws.String(Id),
		SnapshotRetentionLimit: awsResp.CacheClusters[0].SnapshotRetentionLimit,
		SnapshotWindow:         awsResp.CacheClusters[0].SnapshotWindow,
		// -- Tags: tagsResp.TagList - unable to get tags ATM.
	})
	if err != nil {
		// TODO: try and restore the old one?
		glog.Errorf("ERROR: Unable to restore redis with %s, old snapshot at %s for resource: %s: %s\n", Id, renamedId, instance.Id, err.Error())
		return err
	}

	err = provider.awssvc.WaitUntilCacheClusterAvailable(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: aws.String(instance.Name),
		MaxRecords:     aws.Int64(20),
	})
	if err != nil {
		glog.Errorf("ERROR: Waiting for the existing cache cluster: %s %s\n", renamedId, err.Error())
		return err
	}

	return err
}
