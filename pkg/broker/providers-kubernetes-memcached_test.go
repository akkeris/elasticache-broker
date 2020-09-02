package broker

import (
	"context"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	// "fmt"
	// "net/url"
	// "encoding/json"
)

func TestKubernetesMemcachedProvision(t *testing.T) {
	var namePrefix = "test"
	var logic *BusinessLogic
	var catalog *broker.CatalogResponse
	var plan osb.Plan
	var highPlan osb.Plan
	var instanceId string = RandomString(12)
	var err error

	os.Setenv("TEST", "true")
	os.Setenv("USE_KUBERNETES", "true")

	Convey("Given a provisioner on kubernetes.", t, func() {
		logic, err = NewBusinessLogic(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: namePrefix})
		So(err, ShouldBeNil)
		So(logic, ShouldNotBeNil)

		Convey("Ensure the initialization for the database works", func() {
			storage, err := InitStorage(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: namePrefix})
			So(err, ShouldBeNil)
			So(storage, ShouldNotBeNil)
		})

		Convey("Ensure we can get the catalog and target plan exists", func() {
			rc := broker.RequestContext{}
			catalog, err = logic.GetCatalog(&rc)
			So(err, ShouldBeNil)
			So(catalog, ShouldNotBeNil)
			So(len(catalog.Services), ShouldEqual, 2)

			var service *osb.Service = nil
			for _, s := range catalog.Services {
				if s.Name == "akkeris-memcached" {
					service = &s
				}
			}
			So(service, ShouldNotBeNil)

			var foundStandard0 = false
			for _, p := range service.Plans {
				if p.Name == "standard-0" {
					plan = p
					foundStandard0 = true
				}
			}
			So(foundStandard0, ShouldEqual, true)

			var foundStandard1 = false
			for _, p := range service.Plans {
				if p.Name == "standard-1" {
					highPlan = p
					foundStandard1 = true
				}
			}
			So(foundStandard1, ShouldEqual, true)
			So(plan, ShouldNotBeNil)
			So(highPlan, ShouldNotBeNil)
		})

		Convey("Ensure kubernetes provisioner can provision a memcached instance", func() {
			var request osb.ProvisionRequest
			var c broker.RequestContext
			request.AcceptsIncomplete = false
			res, err := logic.Provision(&request, &c)
			So(res, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "Status: 422; ErrorMessage: <nil>; Description: The query parameter accepts_incomplete=true MUST be included the request.; ResponseError: AsyncRequired")

			request.AcceptsIncomplete = true
			request.PlanID = "does not exist"
			request.InstanceID = "asfdasdf"
			res, err = logic.Provision(&request, &c)
			So(err.Error(), ShouldEqual, "Status: 404; ErrorMessage: <nil>; Description: Not Found; ResponseError: <nil>")

			request.InstanceID = instanceId
			request.PlanID = plan.ID
			res, err = logic.Provision(&request, &c)

			So(err, ShouldBeNil)
			So(res, ShouldNotBeNil)
		})

		Convey("Get and create service bindings on kubernetes memcached", func() {
			var request osb.LastOperationRequest = osb.LastOperationRequest{InstanceID: instanceId}
			var c broker.RequestContext
			res, err := logic.LastOperation(&request, &c)
			So(err, ShouldBeNil)
			So(res, ShouldNotBeNil)
			So(res.State, ShouldEqual, osb.StateSucceeded)

			var guid = "123e4567-e89b-12d3-a456-426655440000"
			var resource osb.BindResource = osb.BindResource{AppGUID: &guid}
			var brequest osb.BindRequest = osb.BindRequest{InstanceID: instanceId, BindingID: "foo", BindResource: &resource}
			dres, err := logic.Bind(&brequest, &c)
			So(err, ShouldBeNil)
			So(dres, ShouldNotBeNil)
			So(dres.Credentials["MEMCACHED_URL"].(string), ShouldEndWith, "memcached-system.svc.cluster.local:11211")

			var gbrequest osb.GetBindingRequest = osb.GetBindingRequest{InstanceID: instanceId, BindingID: "foo"}
			gbres, err := logic.GetBinding(&gbrequest, &c)
			So(err, ShouldBeNil)
			So(gbres, ShouldNotBeNil)
			So(gbres.Credentials["MEMCACHED_URL"].(string), ShouldEndWith, "memcached-system.svc.cluster.local:11211")
			So(gbres.Credentials["MEMCACHED_URL"].(string), ShouldEqual, dres.Credentials["MEMCACHED_URL"].(string))
		})

		Convey("Ensure unbind for kubernetes memcached works", func() {
			var c broker.RequestContext
			var urequest osb.UnbindRequest = osb.UnbindRequest{InstanceID: instanceId, BindingID: "foo"}
			ures, err := logic.Unbind(&urequest, &c)
			So(err, ShouldBeNil)
			So(ures, ShouldNotBeNil)
		})

		Convey("Ensure deprovisioner for kubernetes memcached works", func() {
			var request osb.LastOperationRequest = osb.LastOperationRequest{InstanceID: instanceId}
			var c broker.RequestContext
			res, err := logic.LastOperation(&request, &c)
			So(err, ShouldBeNil)
			So(res, ShouldNotBeNil)
			So(res.State, ShouldEqual, osb.StateSucceeded)

			var drequest osb.DeprovisionRequest = osb.DeprovisionRequest{InstanceID: instanceId}
			dres, err := logic.Deprovision(&drequest, &c)

			So(err, ShouldBeNil)
			So(dres, ShouldNotBeNil)
		})
	})
}
