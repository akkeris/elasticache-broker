package broker

import (
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"os"
	"strconv"
	"strings"
	"time"
	"fmt"
)

type AWSInstanceRedisProvider struct {
	Provider
	awssvc              *elasticache.ElastiCache
	namePrefix          string
	instanceCache 		map[string]*Instance
}

func NewAWSInstanceRedisProvider(namePrefix string) (*AWSInstanceRedisProvider, error) {
	if os.Getenv("AWS_REGION") == "" {
		return nil, errors.New("Unable to find AWS_REGION environment variable.")
	}
	t := time.NewTicker(time.Second * 5)
	AWSInstanceRedisProvider := &AWSInstanceRedisProvider{
		namePrefix:          namePrefix,
		instanceCache:		 make(map[string]*Instance),
		awssvc:              elasticache.New(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})),
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
	if provider.instanceCache[name + plan.ID] != nil {
		return provider.instanceCache[name + plan.ID], nil
	}
	resp, err := provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: 	aws.String(name),
		MaxRecords: 		aws.Int64(20),
		ShowCacheNodeInfo: 	aws.Bool(true)
	})
	if err != nil {
		return nil, err
	}
	if len(resp.CacheClusters) < 1 || len(resp.CacheClusters[0].CacheNodes) < 1 {
		reutrn nil, errors.New("No cluster or nodes were found in instance.")
	}
	var endpoint = ""
	if resp.CacheClusters[0].CacheNodes[0].Endpoint != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Port != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Address != nil {
		endpoint = *resp.CacheClusters[0].CacheNodes[0].Endpoint.Address + ":" + strconv.FormatInt(*resp.CacheClusters[0].CacheNodes[0].Endpoint.Port, 10)
	}
	provider.instanceCache[name + plan.ID] = &Instance{
		Id:            "", // providers should not store this.
		ProviderId:    *resp.CacheClusters[0].ClusterId,
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

	return provider.instanceCache[name + plan.ID], nil
}

func (provider AWSInstanceRedisProvider) PerformPostProvision(db *Instance) (*Instance, error) {
	return db, nil
}

func (provider AWSInstanceRedisProvider) GetUrl(instance *Instance) map[string]interface{} {
	return map[string]{}interface{
		"MEMCACHED_URL":instance.Scheme + "://" + instance.Endpoint
	}
}

func (provider AWSInstanceRedisProvider) ProvisionWithSettings(Id string, plan *ProviderPlan, settings *elasticache.CreateCacheClusterInput) (*Instance, error) {
	resp, err := provider.awssvc.CreateCacheCluster(settings)
	if err != nil {
		return nil, err
	}

	if len(resp.CacheClusters) < 1 || len(resp.CacheClusters[0].CacheNodes) < 1 {
		reutrn nil, errors.New("No cluster or nodes were found in instance.")
	}

	var endpoint = ""
	if resp.CacheClusters[0].CacheNodes[0].Endpoint != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Port != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Address != nil {
		endpoint = *resp.CacheClusters[0].CacheNodes[0].Endpoint.Address + ":" + strconv.FormatInt(*resp.CacheClusters[0].CacheNodes[0].Endpoint.Port, 10)
	}

	return &Instance{
		Id:            Id,
		Name:          *resp.CacheClusters[0].ClusterId,
		ProviderId:    *resp.CacheClusters[0].ClusterId,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      endpoint,
		Status:        *resp.CacheClusters[0].CacheClusterStatus,
		Ready:         IsReady(*resp.CacheClusters[0].CacheClusterStatus),
		Engine:        *resp.CacheClusters[0].Engine,
		EngineVersion: *resp.CacheClusters[0].EngineVersion,
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
	resp, err := provider.awssvc.DeleteCacheCluster(&elasticache.DeleteCacheClusterInput{
		CacheClusterId: aws.String(instance.ProviderId),
	})
	return err
}

func (provider AWSInstanceRedisProvider) ModifyWithSettings(Instance *Instance, plan *ProviderPlan, settings *elasticache.CreateCacheClusterInput) (*Instance, error) {
	resp, err := provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: 	aws.String(Instance.ProviderId),
		MaxRecords: 		aws.Int64(20),
		ShowCacheNodeInfo: 	aws.Bool(true)
	})
	if err != nil {
		return nil, err
	}
	if len(resp.CacheClusters) < 1 || len(resp.CacheClusters[0].CacheNodes) < 1 {
		reutrn nil, errors.New("No cluster or nodes were found in instance.")
	}
	glog.Infof("Instance: %s modifying settings...\n", Instance.Id)
	resp, err := provider.awssvc.ModifyInstance(&elasticache.ModifyCacheClusterInput{
		AZMode:        				settings.AZmode,
		ApplyImmediately: 			aws.Bool(true),
		AutoMinorVersionUpgrade:	settings.AutoMinorVersionUpgrade,
		CacheClusterId:				aws.String(Instance.ProviderId),
		CacheNodeType:         	 	settings.CacheNodeType,
		CacheParameterGroupName:   	settings.CacheParameterGroupName,
		CacheSecurityGroupNames:    settings.CacheSecurityGroupNames,
		EngineVersion:      		settings.EngineVersion,
		NotificationTopicArn:		settings.NotificationTopicArn,
		NotificationTopicStatus:	settings.NotificationTopicStatus,
		NumCacheNodes:     			settings.NumCacheNodes,
		PreferredMaintenanceWindow: settings.PreferredMaintenanceWindow,
		SecurityGroupIds:			settings.SecurityGroupIds,
		SnapshotRetentionLimit:   	settings.SnapshotRetentionLimit,
		SnapshotWindow:             settings.SnapshotWindow,
	})
	if err != nil {
		return nil, err
	}

	tick := time.NewTicker(time.Second * 30)
	<-tick.C

	var endpoint = ""
	if resp.CacheClusters[0].CacheNodes[0].Endpoint != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Port != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Address != nil {
		endpoint = *resp.CacheClusters[0].CacheNodes[0].Endpoint.Address + ":" + strconv.FormatInt(*resp.CacheClusters[0].CacheNodes[0].Endpoint.Port, 10)
	}

	resp, err = provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: 	aws.String(Instance.ProviderId),
		MaxRecords: 		aws.Int64(20),
		ShowCacheNodeInfo: 	aws.Bool(true)
	})
	if err != nil {
		return nil, err
	}
	if len(resp.CacheClusters) < 1 || len(resp.CacheClusters[0].CacheNodes) < 1 {
		reutrn nil, errors.New("No cluster or nodes were found in instance after modification!")
	}
	glog.Infof("Instance: %s modifications finished.\n", Instance.Id)
	return &Instance{
		Id:            Instance.Id,
		Name:          Instance.Name,
		ProviderId:    *resp.CacheClusters[0].ClusterId,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      endpoint,
		Status:        *resp.CacheClusters[0].CacheClusterStatus,
		Ready:         IsReady(*resp.CacheClusters[0].CacheClusterStatus),
		Engine:        *resp.CacheClusters[0].Engine,
		EngineVersion: *resp.CacheClusters[0].EngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider AWSInstanceRedisProvider) Modify(Instance *Instance, plan *ProviderPlan) (*Instance, error) {
	if !CanBeModified(Instance.Status) {
		return nil, errors.New("Databases cannot be modifed during backups, upgrades or while maintenance is being performed.")
	}
	/*
	var settings elasticache.CreateCacheClusterInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	var oldSettings elasticache.CreateCacheClusterInput 
	if err := json.Unmarshal([]byte(Instance.Plan.providerPrivateDetails), &oldSettings); err != nil {
		return nil, err
	}
	*/
	return provider.ModifyWithSettings(Instance, plan, &settings)
}

func (provider AWSInstanceRedisProvider) Tag(Instance *Instance, Name string, Value string) error {
	// TODO: Support multiple values of the same tag name, comma delimit them.
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
	// TODO: Support multiple values of the same tag name, comma delimit them.
	_, err := provider.awssvc.RemoveTagsFromResource(&elasticache.RemoveTagsFromResourceInput{
		ResourceName: aws.String(Instance.ProviderId),
		TagKeys: []*string{
			aws.String(Name),
		},
	})
	return err
}

func (provider AWSInstanceRedisProvider) Restart(Instance *Instance) error {
	// What about replica?
	if !Instance.Ready {
		return errors.New("Cannot restart a database that is unavailable.")
	}
	_, err := provider.awssvc.RebootInstance(&elasticache.RebootCacheCluster{
		CacheClusterId: aws.String(Instance.ProvideId),
		CacheNodeIdsToReboot: []string{"all"},
	})
	return err
}

func (provider AWSInstanceRedisProvider) Flush(Instance *instance) error {
	return errors.New("Redis does not support flush operations.")
}

func (provider AWSInstanceRedisProvider) Stats(Instance *instance) ([]Stat, error) {
	return errors.New("Redis does not support flush stats.")
}
