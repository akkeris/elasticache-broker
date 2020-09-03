package broker

import (
	"encoding/json"
	"errors"
	v1apps "k8s.io/api/apps/v1"
	v1core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"path/filepath"
	"strings"
	"time"
	"github.com/go-redis/redis"
	"k8s.io/client-go/kubernetes/fake"
)

type KubernetesInstanceRedisProvider struct {
	Provider
	kubernetes    kubernetes.Interface
	namePrefix    string
	instanceCache map[string]*Instance
}

type redisProviderPlan struct {
	SizeInMegabytes string `json:"size_in_megabytes"`
	Version         string `json:"version"`
}

var namespaceRedis string = "redis-system"

func NewKubernetesInstanceRedisProvider(namePrefix string) (*KubernetesInstanceRedisProvider, error) {
	var provider KubernetesInstanceRedisProvider = KubernetesInstanceRedisProvider{
		namePrefix:    namePrefix,
		instanceCache: make(map[string]*Instance),
		kubernetes:    nil,
	}
	if os.Getenv("TEST") == "true" {
		if fakeClient == nil {
			fakeClient = fake.NewSimpleClientset()
		}
		provider.kubernetes = fakeClient
	} else {
		config, err := rest.InClusterConfig()
		if err != nil {
			// An explicit instruction must be given to allow us to use the local kube config.
			if os.Getenv("USE_LOCAL_KUBE_CONTEXT") == "true" {
				config, err = clientcmd.BuildConfigFromFlags("", filepath.Join(homeDir(), ".kube", "config"))
				if err != nil {
					panic(err.Error())
				}
			} else {
				panic(err.Error())
			}
		}

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			panic(err.Error())
		}
		provider.kubernetes = clientset
	}

	t := time.NewTicker(time.Second * 5)
	go (func() {
		for {
			provider.instanceCache = make(map[string]*Instance)
			<-t.C
		}
	})()

	return &provider, nil
}

func (provider KubernetesInstanceRedisProvider) GetInstance(name string, plan *ProviderPlan) (*Instance, error) {
	result, err := provider.kubernetes.AppsV1().Deployments(namespaceRedis).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var settings redisProviderPlan
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	status := "unknown"
	if len(result.Status.Conditions) > 0 {
		status = result.Status.Conditions[0].Message
	}
	provider.instanceCache[name+plan.ID] = &Instance{
		Id:            "", // providers should not store this.
		ProviderId:    name,
		Name:          name,
		Plan:          plan,
		Username:      "", // providers should not store this.
		Password:      "", // providers should not store this.
		Endpoint:      name + "." + namespaceRedis + ".svc.cluster.local:6379",
		Status:        status,
		Ready:         IsReadyKubernetes(result),
		Engine:        "redis",
		EngineVersion: settings.Version,
		Scheme:        plan.Scheme,
	}

	return provider.instanceCache[name+plan.ID], nil
}

func (provider KubernetesInstanceRedisProvider) PerformPostProvision(db *Instance) (*Instance, error) {
	return db, nil
}

func (provider KubernetesInstanceRedisProvider) GetUrl(instance *Instance) map[string]interface{} {
	return map[string]interface{}{
		"REDIS_URL": instance.Scheme + "://" + instance.Endpoint,
	}
}

func (provider KubernetesInstanceRedisProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*Instance, error) {
	var settings redisProviderPlan
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	limits := v1core.ResourceList{}
	qty, err := resource.ParseQuantity(settings.SizeInMegabytes + "Mi")
	if err != nil {
		return nil, err
	}
	limits[v1core.ResourceMemory] = qty
	name := provider.namePrefix + strings.ToLower(RandomString(9))
	pod := v1core.PodTemplateSpec{
		Spec: v1core.PodSpec{
			Containers: []v1core.Container{
				v1core.Container{
					Name:  "redis",
					Image: "redis:" + settings.Version,
					Resources: v1core.ResourceRequirements{
						Limits: limits,
					},
					Ports: []v1core.ContainerPort{
						v1core.ContainerPort{
							ContainerPort: 6379,
							Protocol:      v1core.ProtocolTCP,
						},
					},
				},
			},
		},
	}
	pod.SetName(name)
	pod.SetNamespace(namespaceRedis)
	pod.SetLabels(map[string]string{"app": name})
	pod.SetAnnotations(map[string]string{"owner": Owner})

	var replicas int32 = 1
	selector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			"app": name,
		},
	}

	deployment := v1apps.Deployment{
		Spec: v1apps.DeploymentSpec{
			Replicas: &replicas,
			Template: pod,
			Selector: &selector,
		},
	}
	deployment.SetName(name)
	deployment.SetNamespace(namespaceRedis)
	deployment.SetLabels(map[string]string{"app": name})
	deployment.SetAnnotations(map[string]string{"owner": Owner})

	result, err := provider.kubernetes.AppsV1().Deployments(namespaceRedis).Create(&deployment)
	if err != nil {
		return nil, err
	}

	// Create the service
	service := v1core.Service{
		Spec: v1core.ServiceSpec{
			Type: v1core.ServiceTypeNodePort,
			Ports: []v1core.ServicePort{
				v1core.ServicePort{
					Port: 6379,
					TargetPort: intstr.FromInt(6379),
				},
			},
			Selector: map[string]string{
				"app": name,
			},
		},
	}
	service.SetName(name)
	service.SetNamespace(namespaceRedis)
	service.SetLabels(map[string]string{"app": name})
	service.SetAnnotations(map[string]string{"owner": Owner})

	if _, err = provider.kubernetes.CoreV1().Services(namespaceRedis).Create(&service); err != nil {
		return nil, err
	}

	return &Instance{
		Id:            Id,
		Name:          name,
		ProviderId:    name,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      name + "." + namespaceRedis + ".svc.cluster.local:6379",
		Status:        "creating",
		Ready:         IsReadyKubernetes(result),
		Engine:        "redis",
		EngineVersion: settings.Version,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider KubernetesInstanceRedisProvider) Deprovision(Instance *Instance, takeSnapshot bool) error {
	err := provider.kubernetes.CoreV1().Services(namespaceRedis).Delete(Instance.Name, &metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	err = provider.kubernetes.AppsV1().Deployments(namespaceRedis).Delete(Instance.Name, &metav1.DeleteOptions{})
	return err
}

func (provider KubernetesInstanceRedisProvider) Modify(Instance *Instance, plan *ProviderPlan) (*Instance, error) {
	result, err := provider.kubernetes.AppsV1().Deployments(namespaceRedis).Get(Instance.ProviderId, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if err := provider.Deprovision(Instance, true); err != nil {
		return nil, err
	}
	return provider.Provision(Instance.Id, plan, result.Annotations["owner"])
}

func (provider KubernetesInstanceRedisProvider) Tag(Instance *Instance, Name string, Value string) error {
	result, err := provider.kubernetes.AppsV1().Deployments(namespaceRedis).Get(Instance.ProviderId, metav1.GetOptions{})
	if err != nil {
		return err
	}
	result.Annotations[Name] = Value
	_, err = provider.kubernetes.AppsV1().Deployments(namespaceRedis).Update(result)
	return err
}

func (provider KubernetesInstanceRedisProvider) Untag(Instance *Instance, Name string) error {
	result, err := provider.kubernetes.AppsV1().Deployments(namespaceRedis).Get(Instance.ProviderId, metav1.GetOptions{})
	if err != nil {
		return err
	}
	delete(result.Annotations, Name)
	_, err = provider.kubernetes.AppsV1().Deployments(namespaceRedis).Update(result)
	return err
}

func (provider KubernetesInstanceRedisProvider) Restart(Instance *Instance) error {
	return provider.kubernetes.CoreV1().Pods(namespaceRedis).DeleteCollection(&metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "app=" + Instance.ProviderId})
}

func (provider KubernetesInstanceRedisProvider) Flush(Instance *Instance) error {
	return errors.New("Flush is not available on redis instances.")
}

func (provider KubernetesInstanceRedisProvider) Stats(Instance *Instance) ([]Stat, error) {
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

func (provider KubernetesInstanceRedisProvider) GetBackup(*Instance, string) (*BackupSpec, error) {
	return nil, errors.New("Backups are unavailable on ephemeral redis")
}

func (provider KubernetesInstanceRedisProvider) ListBackups(*Instance) ([]BackupSpec, error) {
	return nil, errors.New("Backups are unavailable on ephemeral redis")
}

func (provider KubernetesInstanceRedisProvider) CreateBackup(*Instance) (*BackupSpec, error) {
	return nil, errors.New("Backups are unavailable on ephemeral redis")
}

func (provider KubernetesInstanceRedisProvider) RestoreBackup(*Instance, string) error {
	return errors.New("Backups are unavailable on ephemeral redis")
}
