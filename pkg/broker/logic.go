package broker

import (
	"context"
	"encoding/json"
	"github.com/golang/glog"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
	"strings"
)

type BusinessLogic struct {
	ActionBase
	storage    Storage
	namePrefix string
}

func NewBusinessLogic(ctx context.Context, o Options) (*BusinessLogic, error) {
	storage, namePrefix, err := InitFromOptions(ctx, o)
	if err != nil {
		return nil, err
	}

	bl := BusinessLogic{
		storage:    storage,
		namePrefix: namePrefix,
	}

	bl.AddActions("list_backups", "backups", "GET", bl.ActionListBackups)
	bl.AddActions("get_backup", "backups/{backup}", "GET", bl.ActionGetBackup)
	bl.AddActions("create_backup", "backups", "POST", bl.ActionCreateBackup)
	bl.AddActions("restore_backup", "backups/{backup}", "PUT", bl.ActionRestoreBackup)

	bl.AddActions("flush", "flush", "POST", bl.ActionFlushData)
	bl.AddActions("stats", "stats", "POST", bl.ActionGetStats)
	bl.AddActions("restart", "restart", "POST", bl.ActionRestart)
	return &bl, nil
}

func (b *BusinessLogic) ActionRestoreBackup(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	instance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, instance.Plan)
	if err != nil {
		glog.Errorf("Unable to restore backups, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	_, err = provider.GetBackup(instance, vars["backup"])
	if err != nil {
		glog.Errorf("Unable to find backup to restore: %s: %s\n", vars["backup"], err.Error())
		return nil, NotFound()
	}
	byteData, err := json.Marshal(RestoreTaskMetadata{Backup: vars["backup"]})
	if err != nil {
		glog.Errorf("Error: failed to marshal webhook task metadata: %s\n", err)
		return nil, InternalServerError()
	}
	if _, err = b.storage.AddTask(instance.Id, RestoreTask, string(byteData)); err != nil {
		glog.Errorf("Error: Unable to schedule restore backup! (%s): %s\n", instance.Name, err.Error())
		return nil, InternalServerError()
	}
	return map[string]interface{}{"status": "OK"}, nil
}

func (b *BusinessLogic) ActionCreateBackup(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	instance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	if !CanBeModified(instance.Status) {
		return nil, UnprocessableEntityWithMessage("ServiceNotYetAvailable", "A backup cannot be created while this service is under maintenance.")
	}
	provider, err := GetProviderByPlan(b.namePrefix, instance.Plan)
	if err != nil {
		glog.Errorf("Unable to create backup, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	backup, err := provider.CreateBackup(instance)
	if err != nil {
		glog.Errorf("Unable to create backup, create backup failed: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return backup, nil
}

func (b *BusinessLogic) ActionListBackups(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	instance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, instance.Plan)
	if err != nil {
		glog.Errorf("Unable to list backups, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	backups, err := provider.ListBackups(instance)
	if err != nil {
		glog.Errorf("Unable to list backups, create backup failed: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return backups, nil
}

func (b *BusinessLogic) ActionGetBackup(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	instance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, instance.Plan)
	if err != nil {
		glog.Errorf("Unable to create backup, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	backup, err := provider.GetBackup(instance, vars["backup"])
	if err != nil && err.Error() == "Not found" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Unable to get backup, get backup failed: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return backup, nil
}

func (b *BusinessLogic) GetCatalog(c *broker.RequestContext) (*broker.CatalogResponse, error) {
	response := &broker.CatalogResponse{}
	services, err := b.storage.GetServices()
	if err != nil {
		return nil, err
	}
	osbResponse := &osb.CatalogResponse{Services: services}
	response.CatalogResponse = *osbResponse
	return response, nil
}

func (b *BusinessLogic) ActionFlushData(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	Instance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, Instance.Plan)
	if err != nil {
		return nil, InternalServerError()
	}
	provider.Flush(Instance)
	return map[string]string{"flush_all": "ok"}, nil
}

func (b *BusinessLogic) ActionGetStats(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	Instance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, Instance.Plan)
	if err != nil {
		return nil, InternalServerError()
	}
	result, err := provider.Stats(Instance)
	if err != nil {
		glog.Errorf("Unable to pull stats: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return map[string][]Stat{"stats": result}, nil
}

func (b *BusinessLogic) ActionRestart(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	Instance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, Instance.Plan)
	if err != nil {
		return nil, InternalServerError()
	}
	if err := provider.Restart(Instance); err != nil {
		glog.Errorf("Unable to restart: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return map[string]string{"restart": "ok"}, nil
}

func GetInstanceById(namePrefix string, storage Storage, Id string) (*Instance, error) {
	entry, err := storage.GetInstance(Id)
	if err != nil {
		return nil, err
	}

	plan, err := storage.GetPlanByID(entry.PlanId)
	if err != nil {
		return nil, err
	}

	provider, err := GetProviderByPlan(namePrefix, plan)
	if err != nil {
		return nil, err
	}

	Instance, err := provider.GetInstance(entry.Name, plan)
	if err != nil {
		return nil, err
	}

	Instance.Id = entry.Id
	Instance.Username = entry.Username
	Instance.Password = entry.Password
	Instance.Plan = plan

	return Instance, nil
}

func (b *BusinessLogic) GetInstanceById(Id string) (*Instance, error) {
	return GetInstanceById(b.namePrefix, b.storage, Id)
}

func (b *BusinessLogic) GetUnclaimedInstance(PlanId string, InstanceId string) (*Instance, error) {
	Entry, err := b.storage.GetUnclaimedInstance(PlanId, InstanceId)
	if err != nil {
		return nil, err
	}
	Instance, err := b.GetInstanceById(Entry.Id)
	if err != nil {
		if err = b.storage.ReturnClaimedInstance(Entry.Id); err != nil {
			return nil, err
		}
		return nil, err
	}
	return Instance, nil
}

// A peice of advice, never try to make this syncronous by waiting for a to return a response. The problem is
// that can take up to 10 minutes in my experience (depending on the provider), and aside from the API call timing
// out the other issue is it can cause the mutex lock to make the entire API unresponsive.
func (b *BusinessLogic) Provision(request *osb.ProvisionRequest, c *broker.RequestContext) (*broker.ProvisionResponse, error) {
	b.Lock()
	defer b.Unlock()
	response := broker.ProvisionResponse{}

	if !request.AcceptsIncomplete {
		return nil, UnprocessableEntityWithMessage("AsyncRequired", "The query parameter accepts_incomplete=true MUST be included the request.")
	}
	if request.InstanceID == "" {
		return nil, UnprocessableEntityWithMessage("InstanceRequired", "The instance ID was not provided.")
	}

	// Ensure we are not trying to provision a UUID that has ever been used before.
	if err := b.storage.ValidateInstanceID(request.InstanceID); err != nil {
		return nil, UnprocessableEntityWithMessage("InstanceInvalid", "The instance ID was either already in-use or invalid.")
	}

	plan, err := b.storage.GetPlanByID(request.PlanID)
	if err != nil && err.Error() == "Not found" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Unable to provision (GetPlanByID failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	Instance, err := b.GetInstanceById(request.InstanceID)

	if err == nil {
		if Instance.Plan.ID != request.PlanID {
			return nil, ConflictErrorWithMessage("InstanceID in use")
		}
		response.Exists = true
	} else if err != nil && err.Error() == "Cannot find resource instance" {
		response.Exists = false
		Instance, err = b.GetUnclaimedInstance(request.PlanID, request.InstanceID)

		if err != nil && err.Error() == "Cannot find resource instance" {
			// Create a new one
			provider, err := GetProviderByPlan(b.namePrefix, plan)
			if err != nil {
				glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
				return nil, InternalServerError()
			}
			Instance, err = provider.Provision(request.InstanceID, plan, request.OrganizationGUID)
			if err != nil {
				glog.Errorf("Error provisioning resource: %s\n", err.Error())
				return nil, InternalServerError()
			}

			if err = b.storage.AddInstance(Instance); err != nil {
				glog.Errorf("Error inserting record into provisioned table: %s\n", err.Error())

				if err = provider.Deprovision(Instance, false); err != nil {
					glog.Errorf("Error cleaning up (deprovision failed) after insert record failed but provision succeeded (Resource Id:%s Name: %s) %s\n", Instance.Id, Instance.Name, err.Error())
					if _, err = b.storage.AddTask(Instance.Id, DeleteTask, Instance.Name); err != nil {
						glog.Errorf("Error: Unable to add task to delete instance, WE HAVE AN ORPHAN! (%s): %s\n", Instance.Name, err.Error())
					}
				}
				return nil, InternalServerError()
			}
			if !IsAvailable(Instance.Status) {
				if _, err = b.storage.AddTask(Instance.Id, PerformPostProvisionTask, ""); err != nil {
					glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", Instance.Name, err.Error())
				}
				// This is a hack to support callbacks, hopefully this will become an OSB standard.
				if c != nil && c.Request != nil && c.Request.URL != nil && c.Request.URL.Query().Get("webhook") != "" && c.Request.URL.Query().Get("secret") != "" {
					// Schedule a callback
					byteData, err := json.Marshal(WebhookTaskMetadata{Url: c.Request.URL.Query().Get("webhook"), Secret: c.Request.URL.Query().Get("secret")})
					if err != nil {
						glog.Errorf("Error: failed to marshal webhook task metadata: %s\n", err)
					}
					if _, err = b.storage.AddTask(Instance.Id, NotifyCreateServiceWebhookTask, string(byteData)); err != nil {
						glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", Instance.Name, err.Error())
					}
				}
			}
		} else if err != nil {
			glog.Errorf("Got fatal error from unclaimed instance endpoint: %s\n", err.Error())
			return nil, InternalServerError()
		}
	} else {
		glog.Errorf("Unable to get instances: %s\n", err.Error())
		return nil, InternalServerError()
	}

	if request.AcceptsIncomplete && Instance.Ready == false {
		opkey := osb.OperationKey(request.InstanceID)
		response.Async = !Instance.Ready
		response.OperationKey = &opkey
	} else if request.AcceptsIncomplete && Instance.Ready == true {
		response.Async = false
	}

	response.ExtensionAPIs = b.ConvertActionsToExtensions(Instance.Id)

	return &response, nil
}

func (b *BusinessLogic) Deprovision(request *osb.DeprovisionRequest, c *broker.RequestContext) (*broker.DeprovisionResponse, error) {
	b.Lock()
	defer b.Unlock()

	response := broker.DeprovisionResponse{}
	Instance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find resource instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during deprovision) from provisioned table: %s\n", err.Error())
		return nil, InternalServerError()
	}

	provider, err := GetProviderByPlan(b.namePrefix, Instance.Plan)
	if err != nil {
		glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	if err = provider.Deprovision(Instance, true); err != nil {
		glog.Errorf("Error failed to deprovision: (Id: %s Name: %s) %s\n", Instance.Id, Instance.Name, err.Error())
		if _, err = b.storage.AddTask(Instance.Id, DeleteTask, Instance.Name); err != nil {
			glog.Errorf("Error: Unable to schedule delete from provider! (%s): %s\n", Instance.Name, err.Error())
			return nil, InternalServerError()
		} else {
			glog.Errorf("Successfully scheduled db to be removed.")
			response.Async = true
			return &response, nil
		}
	}
	if err = b.storage.DeleteInstance(Instance); err != nil {
		glog.Errorf("Error removing record from provisioned table: %s\n", err.Error())
		return nil, InternalServerError()
	}
	response.Async = false
	return &response, nil
}

func (b *BusinessLogic) Update(request *osb.UpdateInstanceRequest, c *broker.RequestContext) (*broker.UpdateInstanceResponse, error) {
	response := broker.UpdateInstanceResponse{}
	if !request.AcceptsIncomplete {
		return nil, UnprocessableEntity()
	}
	Instance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find resource instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during deprovision) from provisioned table: %s\n", err.Error())
		return nil, InternalServerError()
	}
	if request.PlanID == nil {
		return nil, UnprocessableEntity()
	}

	if !IsAvailable(Instance.Status) {
		return nil, UnprocessableEntityWithMessage("ConcurrencyError", "Clients MUST wait until pending requests have completed for the specified resources.")
	}

	if strings.ToLower(*request.PlanID) == strings.ToLower(Instance.Plan.ID) {
		return nil, UnprocessableEntityWithMessage("UpgradeError", "Cannot upgrade to the same plan.")
	}

	target_plan, err := b.storage.GetPlanByID(*request.PlanID)
	if err != nil {
		glog.Errorf("Unable to provision resource (GetPlanByID failed): %s\n", err.Error())
		return nil, err
	}

	if (Instance.Plan.Provider == target_plan.Provider) || (Instance.Plan.Provider != target_plan.Provider && Instance.Engine == "memcached") {
		byteData, err := json.Marshal(ChangePlansTaskMetadata{Plan: *request.PlanID})
		if err != nil {
			glog.Errorf("Unable to marshal change plans task meta data: %s\n", err.Error())
			return nil, err
		}
		if _, err = b.storage.AddTask(Instance.Id, ChangePlansTask, string(byteData)); err != nil {
			glog.Errorf("Error: Unable to schedule upgrade of a plan! (%s): %s\n", Instance.Name, err.Error())
			return nil, err
		}
		response.Async = true
		return &response, nil
	} else {
		return nil, UnprocessableEntityWithMessage("UpgradeError", "Cannot upgrade or change redis plans across provider types.")
	}
}

func (b *BusinessLogic) LastOperation(request *osb.LastOperationRequest, c *broker.RequestContext) (*broker.LastOperationResponse, error) {
	response := broker.LastOperationResponse{}

	upgrading, err := b.storage.IsUpgrading(request.InstanceID)
	if err != nil {
		glog.Errorf("Unable to get resource (%s) status, IsUpgrading failed: %s\n", request.InstanceID, err.Error())
		return nil, InternalServerError()
	}

	restoring, err := b.storage.IsRestoring(request.InstanceID)
	if err != nil {
		glog.Errorf("Unable to get resource (%s) status, IsRestoring failed: %s\n", request.InstanceID, err.Error())
		return nil, InternalServerError()
	}

	if upgrading {
		desc := "upgrading"
		Instance, err := b.GetInstanceById(request.InstanceID)
		if err == nil && !IsAvailable(Instance.Status) {
			desc = Instance.Status
		}
		response.Description = &desc
		response.State = osb.StateInProgress
		return &response, nil
	} else if restoring {
		desc := "restoring"
		Instance, err := b.GetInstanceById(request.InstanceID)
		if err == nil && !IsAvailable(Instance.Status) {
			desc = Instance.Status
		}
		response.Description = &desc
		response.State = osb.StateInProgress
		return &response, nil
	}

	Instance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find resource instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Unable to get resource (%s) status: %s\n", request.InstanceID, err.Error())
		return nil, InternalServerError()
	}

	b.storage.UpdateInstance(Instance, Instance.Plan.ID)

	if Instance.Ready == true {
		response.Description = &Instance.Status
		response.State = osb.StateSucceeded
	} else if InProgress(Instance.Status) {
		response.Description = &Instance.Status
		response.State = osb.StateInProgress
	} else {
		response.Description = &Instance.Status
		response.State = osb.StateFailed
	}
	return &response, nil
}

func (b *BusinessLogic) Bind(request *osb.BindRequest, c *broker.RequestContext) (*broker.BindResponse, error) {
	b.Lock()
	defer b.Unlock()
	Instance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find resource instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during getbinding): %s\n", err.Error())
		return nil, InternalServerError()
	}
	if Instance.Ready == false {
		return nil, UnprocessableEntity()
	}

	provider, err := GetProviderByPlan(b.namePrefix, Instance.Plan)
	if err != nil {
		glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	if request.BindResource != nil && request.BindResource.AppGUID != nil {
		if err = provider.Tag(Instance, "Binding", request.BindingID); err != nil {
			glog.Errorf("Error tagging: %s with %s, got %s\n", request.InstanceID, *request.BindResource.AppGUID, err.Error())
			return nil, InternalServerError()
		}
		if err = provider.Tag(Instance, "App", *request.BindResource.AppGUID); err != nil {
			glog.Errorf("Error tagging: %s with %s, got %s\n", request.InstanceID, *request.BindResource.AppGUID, err.Error())
			return nil, InternalServerError()
		}
	}

	return &broker.BindResponse{
		BindResponse: osb.BindResponse{
			Async:       false,
			Credentials: provider.GetUrl(Instance),
		},
	}, nil
}

func (b *BusinessLogic) Unbind(request *osb.UnbindRequest, c *broker.RequestContext) (*broker.UnbindResponse, error) {
	b.Lock()
	defer b.Unlock()

	Instance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find resource instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during getbinding): %s\n", err.Error())
		return nil, InternalServerError()
	}
	if Instance.Ready == false {
		return nil, UnprocessableEntity()
	}

	provider, err := GetProviderByPlan(b.namePrefix, Instance.Plan)
	if err != nil {
		glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	if err = provider.Untag(Instance, "Binding"); err != nil {
		glog.Errorf("Error untagging: %s\n", err.Error())
		return nil, InternalServerError()
	}
	if err = provider.Untag(Instance, "App"); err != nil {
		glog.Errorf("Error untagging: got %s\n", err.Error())
		return nil, InternalServerError()
	}

	return &broker.UnbindResponse{
		UnbindResponse: osb.UnbindResponse{
			Async: false,
		},
	}, nil
}

func (b *BusinessLogic) ValidateBrokerAPIVersion(version string) error {
	return nil
}

func (b *BusinessLogic) GetBinding(request *osb.GetBindingRequest, context *broker.RequestContext) (*osb.GetBindingResponse, error) {
	Instance, err := b.GetInstanceById(request.InstanceID)
	if err == nil && !CanGetBindings(Instance.Status) {
		return nil, UnprocessableEntityWithMessage("ServiceNotYetAvailable", "The service requested is not yet available.")
	}
	if err != nil && err.Error() == "Cannot find resource instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during getbinding): %s\n", err.Error())
		return nil, err
	}
	provider, err := GetProviderByPlan(b.namePrefix, Instance.Plan)
	if err != nil {
		glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	return &osb.GetBindingResponse{
		Credentials: provider.GetUrl(Instance),
	}, nil
}

var _ broker.Interface = &BusinessLogic{}
