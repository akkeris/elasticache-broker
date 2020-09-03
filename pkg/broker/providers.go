package broker

import (
	"errors"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"os"
)

type Providers string

const (
	AWSRedisInstance            Providers = "aws-redis-instance"
	AWSMemcachedInstance        Providers = "aws-memcached-instance"
	KubernetesMemcachedInstance Providers = "kubernetes-memcached-instance"
	KubernetesRedisInstance 	Providers = "kubernetes-redis-instance"
	Unknown                     Providers = "unknown"
)

func GetProvidersFromString(str string) Providers {
	if str == "aws-redis-instance" {
		return AWSRedisInstance
	} else if str == "aws-memcached-instance" {
		return AWSMemcachedInstance
	} else if str == "kubernetes-memcached-instance" {
		return KubernetesMemcachedInstance
	} else if str == "kubernetes-redis-instance" {
		return KubernetesRedisInstance
	}
	return Unknown
}

type ProviderPlan struct {
	basePlan               osb.Plan  `json:"-"` /* NEVER allow this to be serialized into a JSON call as it may accidently send sensitive info to callbacks */
	Provider               Providers `json:"provider"`
	providerPrivateDetails string    `json:"-"` /* NEVER allow this to be serialized into a JSON call as it may accidently send sensitive info to callbacks */
	ID                     string    `json:"id"`
	Scheme                 string    `json:"scheme"`
}

type Provider interface {
	GetInstance(string, *ProviderPlan) (*Instance, error)
	Provision(string, *ProviderPlan, string) (*Instance, error)
	Deprovision(*Instance, bool) error
	Modify(*Instance, *ProviderPlan) (*Instance, error)
	Tag(*Instance, string, string) error
	Untag(*Instance, string) error
	Restart(*Instance) error
	PerformPostProvision(*Instance) (*Instance, error)
	GetUrl(*Instance) map[string]interface{}
	Flush(*Instance) error
	Stats(*Instance) ([]Stat, error)
	GetBackup(*Instance, string) (*BackupSpec, error)
	ListBackups(*Instance) ([]BackupSpec, error)
	CreateBackup(*Instance) (*BackupSpec, error)
	RestoreBackup(*Instance, string) error
}

func GetProviderByPlan(namePrefix string, plan *ProviderPlan) (Provider, error) {
	if plan.Provider == AWSRedisInstance {
		return NewAWSInstanceRedisProvider(namePrefix)
	} else if plan.Provider == AWSMemcachedInstance {
		return NewAWSInstanceMemcachedProvider(namePrefix)
	} else if plan.Provider == KubernetesMemcachedInstance && os.Getenv("USE_KUBERNETES") == "true" {
		if os.Getenv("TEST") == "true" {
			return NewKubernetesInstanceMemcachedProvider(namePrefix)
		} else {
			return NewKubernetesInstanceMemcachedProvider(namePrefix)
		}
	} else if plan.Provider == KubernetesRedisInstance && os.Getenv("USE_KUBERNETES") == "true" {
		if os.Getenv("TEST") == "true" {
			return NewKubernetesInstanceRedisProvider(namePrefix)
		} else {
			return NewKubernetesInstanceRedisProvider(namePrefix)
		}
	} else {
		return nil, errors.New("Unable to find provider for plan.")
	}
}
