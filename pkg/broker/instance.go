package broker

import (
	"reflect"
)

type Stat struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Instance struct {
	Id            string        `json:"id"`
	Name          string        `json:"name"`
	ProviderId    string        `json:"provider_id"`
	Plan          *ProviderPlan `json:"plan,omitempty"`
	Username      string        `json:"username"`
	Password      string        `json:"password"`
	Endpoint      string        `json:"endpoint"`
	Status        string        `json:"status"`
	Ready         bool          `json:"ready"`
	Engine        string        `json:"engine"`
	EngineVersion string        `json:"engine_version"`
	Scheme        string        `json:"scheme"`
}

type Entry struct {
	Id       string
	Name     string
	PlanId   string
	Claimed  bool
	Tasks	 int
	Status   string
	Username string
	Password string
	Endpoint string
}

func (i *Instance) Match(other *Instance) bool {
	return reflect.DeepEqual(i, other)
}

type ResourceUrlSpec struct {
	Username string
	Password string
	Endpoint string
	Plan     string
}

type ResourceSpec struct {
	Name string `json:"name"`
}

func IsAvailable(status string) bool {
	return status == "available" ||
			// gcloud status
			status == "RUNNABLE"
}

func IsReady(status string) bool {
	return status == "available" ||
		status == "configuring-enhanced-monitoring" ||
		status == "storage-optimization" ||
		status == "backing-up" ||
		// gcloud states
		status == "RUNNABLE" ||
		status == "UNKNOWN_STATE"
}

func InProgress(status string) bool {
	return status == "creating" || status == "starting" || status == "modifying" ||
		status == "rebooting" || status == "moving-to-vpc" ||
		status == "renaming" || status == "upgrading" || status == "backtracking" ||
		status == "maintenance" || status == "resetting-master-credentials" ||
		// gcloud states
		status == "PENDING_CREATE" || status == "MAINTENANCE"

}

func CanGetBindings(status string) bool {
	// Should we potentially add upgrading to this list?
	return  status != "creating" && status != "starting" && 
			status != "stopping" && status != "stopped" && status != "deleting" &&
			// gcloud states
			status != "SUSPENDED" && status != "PENDING_CREATE" && status != "MAINTENANCE" &&
			status != "FAILED" && status != "UNKNOWN_STATE"
}

func CanBeModified(status string) bool {
	// aws states
	return status != "creating" && status != "starting" && status != "modifying" &&
		status != "rebooting" && status != "moving-to-vpc" && status != "backing-up" &&
		status != "renaming" && status != "upgrading" && status != "backtracking" &&
		status != "maintenance" && status != "resetting-master-credentials" &&
		// gcloud states
		status != "SUSPENDED" && status != "PENDING_CREATE" && status != "MAINTENANCE" &&
		status != "FAILED" && status != "UNKNOWN_STATE"
}

func CanBeDeleted(status string) bool {
	return status != "creating" && status != "starting" &&
		status != "rebooting" && status != "moving-to-vpc" && status != "backing-up" &&
		status != "renaming" && status != "upgrading" && status != "backtracking" &&
		status != "maintenance" && status != "resetting-master-credentials" && 
		status != "SUSPENDED" && status != "PENDING_CREATE" && status != "MAINTENANCE" &&
		status != "FAILED" && status != "UNKNOWN_STATE"
}

/** gcloud settings **/
// State: The current serving state of the Cloud SQL instance. This can
    // be one of the following.
    // RUNNABLE: The instance is running, or is ready to run when
    // accessed.
    // SUSPENDED: The instance is not available, for example due to problems
    // with billing.
    // PENDING_CREATE: The instance is being created.
    // MAINTENANCE: The instance is down for maintenance.
    // FAILED: The instance creation failed.
    // UNKNOWN_STATE: The state of the instance is unknown.
