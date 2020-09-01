package broker

import (
	"encoding/json"
	"errors"
	"strings"
	"net"
	"time"
	"io/ioutil"
	"os"
	"path/filepath"
	v1apps "k8s.io/api/apps/v1"
	v1core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/rest"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type KubernetesInstanceMemcachedProvider struct {
	Provider
	kubernetes 			*kubernetes.Clientset
	namePrefix          string
	instanceCache 		map[string]*Instance
}

type MemcachedProviderPlan struct {
	SizeInMegabytes string `json:"size_in_megabytes"`
	Version string `json:"version"`
}

var namespace string = "memcached-system"

func IsReadyKubernetes(dep *v1apps.Deployment) bool {
	return dep.Status.ReadyReplicas == dep.Status.Replicas
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func NewKubernetesInstanceMemcachedProvider(namePrefix string) (*KubernetesInstanceMemcachedProvider, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		config, err = clientcmd.BuildConfigFromFlags("", filepath.Join(homeDir(), ".kube", "config"))
		if err != nil {
			panic(err.Error())
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	t := time.NewTicker(time.Second * 5)

	KubernetesInstanceMemcachedProvider := &KubernetesInstanceMemcachedProvider{
		namePrefix:          namePrefix,
		instanceCache:		 make(map[string]*Instance),
		kubernetes: 		 clientset,
	}

	go (func() {
		for {
			KubernetesInstanceMemcachedProvider.instanceCache = make(map[string]*Instance)
			<-t.C
		}
	})()

	return KubernetesInstanceMemcachedProvider, nil
}

func (provider KubernetesInstanceMemcachedProvider) GetInstance(name string, plan *ProviderPlan) (*Instance, error) {	
	result, err := provider.kubernetes.AppsV1().Deployments(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var settings MemcachedProviderPlan
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	provider.instanceCache[name + plan.ID] = &Instance{
		Id:            "", // providers should not store this.
		ProviderId:    name,
		Name:          name,
		Plan:          plan,
		Username:      "", // providers should not store this.
		Password:      "", // providers should not store this.
		Endpoint:      name + "." + namespace + ".svc.cluster.local:11211",
		Status:        result.Status.Conditions[0].Message,
		Ready:         IsReadyKubernetes(result),
		Engine:        "memcached",
		EngineVersion: settings.Version,
		Scheme:        plan.Scheme,
	}

	return provider.instanceCache[name + plan.ID], nil
}

func (provider KubernetesInstanceMemcachedProvider) PerformPostProvision(db *Instance) (*Instance, error) {
	return db, nil
}

func (provider KubernetesInstanceMemcachedProvider) GetUrl(instance *Instance) map[string]interface{} {
	return map[string]interface{}{
		"MEMCACHED_URL":instance.Endpoint,
	}
}

func (provider KubernetesInstanceMemcachedProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*Instance, error) {
	var settings MemcachedProviderPlan
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
		Spec: v1core.PodSpec {
			Containers: []v1core.Container{
				v1core.Container{
					Name:"memcached",
					Image:"memcached:" + settings.Version,
					Resources: v1core.ResourceRequirements{
						Limits: limits,
					},
					Args: []string{
						"-m " + settings.SizeInMegabytes,
						"-I 50M",
					},
					Ports: []v1core.ContainerPort{
						v1core.ContainerPort{
							ContainerPort: 11211,
							Protocol: v1core.ProtocolTCP,
						},
					},
				},
			},
		},
	}
	pod.SetName(name)
	pod.SetNamespace(namespace)
	pod.SetLabels(map[string]string {"app":name})
	pod.SetAnnotations(map[string]string {"owner":Owner})

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
	deployment.SetNamespace(namespace)
	deployment.SetLabels(map[string]string {"app":name})
	deployment.SetAnnotations(map[string]string {"owner":Owner})

	result, err := provider.kubernetes.AppsV1().Deployments(namespace).Create(&deployment)
	if err != nil {
		return nil, err
	}

	// Create the service
	service := v1core.Service{
		Spec: v1core.ServiceSpec{
			Type: v1core.ServiceTypeNodePort,
			Ports: []v1core.ServicePort{
				v1core.ServicePort{
					Port: 11211,
					TargetPort: intstr.FromInt(11211),
				},
			},
			Selector: map[string]string {
				"app": name,
			},
		},
	}
	service.SetName(name)
	service.SetNamespace(namespace)
	service.SetLabels(map[string]string {"app": name})
	service.SetAnnotations(map[string]string {"owner":Owner})

	
	if _, err = provider.kubernetes.CoreV1().Services(namespace).Create(&service); err != nil {
		return nil, err
	}

	return &Instance{
		Id:            Id,
		Name:          name,
		ProviderId:    name,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      name + "." + namespace + ".svc.cluster.local:11211",
		Status:        "Creating",
		Ready:         IsReadyKubernetes(result),
		Engine:        "memcached",
		EngineVersion: settings.Version,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider KubernetesInstanceMemcachedProvider) Deprovision(Instance *Instance, takeSnapshot bool) error {
	err := provider.kubernetes.CoreV1().Services(namespace).Delete(Instance.Name, &metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	err = provider.kubernetes.AppsV1().Deployments(namespace).Delete(Instance.Name, &metav1.DeleteOptions{})
	return err
}

func (provider KubernetesInstanceMemcachedProvider) Modify(Instance *Instance, plan *ProviderPlan) (*Instance, error) {
	result, err := provider.kubernetes.AppsV1().Deployments(namespace).Get(Instance.ProviderId, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if err := provider.Deprovision(Instance, true); err != nil {
		return nil, err
	}
	return provider.Provision(Instance.Id, plan, result.Annotations["owner"])
}

func (provider KubernetesInstanceMemcachedProvider) Tag(Instance *Instance, Name string, Value string) error {
	result, err := provider.kubernetes.AppsV1().Deployments(namespace).Get(Instance.ProviderId, metav1.GetOptions{})
	if err != nil {
		return err
	}
	result.Annotations[Name] = Value
	_, err = provider.kubernetes.AppsV1().Deployments(namespace).Update(result)
	return err
}

func (provider KubernetesInstanceMemcachedProvider) Untag(Instance *Instance, Name string) error {
	result, err := provider.kubernetes.AppsV1().Deployments(namespace).Get(Instance.ProviderId, metav1.GetOptions{})
	if err != nil {
		return err
	}
	delete(result.Annotations, Name)
	_, err = provider.kubernetes.AppsV1().Deployments(namespace).Update(result)
	return err
}

func (provider KubernetesInstanceMemcachedProvider) Restart(Instance *Instance) error {
	return provider.kubernetes.CoreV1().Pods(namespace).DeleteCollection(&metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "app=" + Instance.ProviderId})
}

func (provider KubernetesInstanceMemcachedProvider) Flush(Instance *Instance) error {
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

func (provider KubernetesInstanceMemcachedProvider) Stats(Instance *Instance) ([]Stat, error) {
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


func (provider KubernetesInstanceMemcachedProvider) GetBackup(*Instance, string) (*BackupSpec, error) {
	return nil, errors.New("Backups are unavailable on a memcached")
}

func (provider KubernetesInstanceMemcachedProvider) ListBackups(*Instance) ([]BackupSpec, error) {
	return nil, errors.New("Backups are unavailable on a memcached")
}

func (provider KubernetesInstanceMemcachedProvider) CreateBackup(*Instance) (*BackupSpec, error) {
	return nil, errors.New("Backups are unavailable on a memcached")
}

func (provider KubernetesInstanceMemcachedProvider) RestoreBackup(*Instance, string) error {
	return errors.New("Backups are unavailable on a memcached")
}
