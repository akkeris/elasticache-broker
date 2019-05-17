package broker

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"os"
	"strconv"
	"strings"
	"time"
	"net"
	"io/ioutil"
)

type AWSInstanceMemcachedProvider struct {
	Provider
	awssvc              *elasticache.ElastiCache
	namePrefix          string
	instanceCache 		map[string]*Instance
}

func NewAWSInstanceMemcachedProvider(namePrefix string) (*AWSInstanceMemcachedProvider, error) {
	if os.Getenv("AWS_REGION") == "" {
		return nil, errors.New("Unable to find AWS_REGION environment variable.")
	}
	t := time.NewTicker(time.Second * 5)
	AWSInstanceMemcachedProvider := &AWSInstanceMemcachedProvider{
		namePrefix:          namePrefix,
		instanceCache:		 make(map[string]*Instance),
		awssvc:              elasticache.New(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})),
	}
	go (func() {
		for {
			AWSInstanceMemcachedProvider.instanceCache = make(map[string]*Instance)
			<-t.C
		}
	})()
	return AWSInstanceMemcachedProvider, nil
}

func (provider AWSInstanceMemcachedProvider) GetInstance(name string, plan *ProviderPlan) (*Instance, error) {
	if provider.instanceCache[name + plan.ID] != nil {
		return provider.instanceCache[name + plan.ID], nil
	}
	resp, err := provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: 	aws.String(name),
		MaxRecords: 		aws.Int64(20),
		ShowCacheNodeInfo: 	aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	var endpoint = ""
	if len(resp.CacheClusters) > 0 && len(resp.CacheClusters[0].CacheNodes) > 0 && resp.CacheClusters[0].CacheNodes[0].Endpoint != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Port != nil && resp.CacheClusters[0].CacheNodes[0].Endpoint.Address != nil {
		endpoint = *resp.CacheClusters[0].CacheNodes[0].Endpoint.Address + ":" + strconv.FormatInt(*resp.CacheClusters[0].CacheNodes[0].Endpoint.Port, 10)
	}
	provider.instanceCache[name + plan.ID] = &Instance{
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

	return provider.instanceCache[name + plan.ID], nil
}

func (provider AWSInstanceMemcachedProvider) PerformPostProvision(db *Instance) (*Instance, error) {
	return db, nil
}

func (provider AWSInstanceMemcachedProvider) GetUrl(instance *Instance) map[string]interface{} {
	return map[string]interface{}{
		"MEMCACHED_URL":instance.Endpoint,
	}
}

func (provider AWSInstanceMemcachedProvider) ProvisionWithSettings(Id string, plan *ProviderPlan, settings *elasticache.CreateCacheClusterInput) (*Instance, error) {
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


func (provider AWSInstanceMemcachedProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*Instance, error) {
	var settings elasticache.CreateCacheClusterInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	settings.CacheClusterId = aws.String(strings.ToLower(provider.namePrefix + RandomString(8)))
	settings.Tags = []*elasticache.Tag{{Key: aws.String("BillingCode"), Value: aws.String(Owner)}}
	return provider.ProvisionWithSettings(Id, plan, &settings)
}

func (provider AWSInstanceMemcachedProvider) Deprovision(Instance *Instance, takeSnapshot bool) error {
	// memcached does not support snapshots.
	_, err := provider.awssvc.DeleteCacheCluster(&elasticache.DeleteCacheClusterInput{
		CacheClusterId: aws.String(Instance.ProviderId),
	})
	return err
}

func (provider AWSInstanceMemcachedProvider) Modify(Instance *Instance, plan *ProviderPlan) (*Instance, error) {
	// Memcached cannot be upgraded really, only a few trivial parameters can be
	// changed, so we'll nuke the old instance, wait for it to die, then create a 
	// new instance with the same identifier.
	if !CanBeModified(Instance.Status) {
		return nil, errors.New("Databases cannot be modifed during backups, upgrades or while maintenance is being performed.")
	}
	var settings elasticache.CreateCacheClusterInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	if err := provider.Deprovision(Instance, false); err != nil {
		return nil, err
	}
	provider.awssvc.WaitUntilCacheClusterDeleted(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: aws.String(Instance.ProviderId),
		MaxRecords: 		aws.Int64(20),
		ShowCacheNodeInfo: 	aws.Bool(true),
	})
	settings.CacheClusterId = aws.String(Instance.ProviderId)
	return provider.ProvisionWithSettings(Instance.Id, plan, &settings)
}

func (provider AWSInstanceMemcachedProvider) Tag(Instance *Instance, Name string, Value string) error {
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

func (provider AWSInstanceMemcachedProvider) Untag(Instance *Instance, Name string) error {
	// TODO: Support multiple values of the same tag name, comma delimit them.
	_, err := provider.awssvc.RemoveTagsFromResource(&elasticache.RemoveTagsFromResourceInput{
		ResourceName: aws.String(Instance.ProviderId),
		TagKeys: []*string{
			aws.String(Name),
		},
	})
	return err
}

func (provider AWSInstanceMemcachedProvider) Restart(Instance *Instance) error {
	if !Instance.Ready {
		return errors.New("Cannot restart a database that is unavailable.")
	}
	res, err := provider.awssvc.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId: 	aws.String(Instance.ProviderId),
		MaxRecords: 		aws.Int64(20),
		ShowCacheNodeInfo: 	aws.Bool(true),
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
		CacheClusterId: aws.String(Instance.ProviderId),
		CacheNodeIdsToReboot: nodes,
	})
	return err
}

func (provider AWSInstanceMemcachedProvider) Flush(Instance *Instance) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", Instance.Endpoint)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	if _, err = conn.Write([]byte("flush_all\n")); err != nil {
		conn.CloseWrite()
		return err
	}
	conn.CloseWrite()
	return nil
}

func (provider AWSInstanceMemcachedProvider) Stats(Instance *Instance) ([]Stat, error) {
	var stats []Stat
	tcpAddr, err := net.ResolveTCPAddr("tcp4", Instance.Endpoint)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	if _, err = conn.Write([]byte("stats\n")); err != nil {
		conn.CloseWrite()
		return nil, err
	}
	conn.CloseWrite()
	result, err := ioutil.ReadAll(conn)
	if err != nil {
		return nil, err
	}
	resulta := strings.Split(string(result), "\n")
	for _, element := range resulta {
		if strings.HasPrefix(element, "STAT") {
			stat := strings.Split(element, " ")
			stats = append(stats, Stat{
				Key:stat[1],
				Value:strings.TrimSpace(stat[2]),
			})
		}
	}
	return stats, nil
}