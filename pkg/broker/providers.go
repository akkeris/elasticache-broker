package broker

import (
	"errors"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
)

type Providers string

const (
	AWSRedisInstance   		Providers = "aws-redis-instance"
	AWSMemcachedInstance    Providers = "aws-memcached-instance"
	Unknown        			Providers = "unknown"
)

func GetProvidersFromString(str string) Providers {
	if str == "aws-redis-instance" {
		return AWSRedisInstance
	} else if str == "aws-memcached-instance" {
		return AWSMemcachedInstance
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
	} else {
		return nil, errors.New("Unable to find provider for plan.")
	}
}
