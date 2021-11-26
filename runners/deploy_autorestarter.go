package runners

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	deployV1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"strconv"
	"time"
)

type restarter struct {
	client        client.Client
	metricsClient MetricsClient
	interval      time.Duration
	log           logr.Logger
	recorder      record.EventRecorder
}

func NewRestarter(c client.Client, log logr.Logger, interval time.Duration, recorder record.EventRecorder) manager.Runnable {

	return &restarter{
		client:   c,
		log:      log,
		interval: interval,
		recorder: recorder,
	}
}

// Start implements manager.Runnable
func (c *restarter) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)

	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := c.reconcile(ctx)
			if err != nil {
				c.log.Error(err, "failed to reconcile")
				return err
			}
		}
	}
}

func (c *restarter) reconcile(ctx context.Context) error {
	stopPvcs, stopPvcErr := c.getPVCListByConditionsType(ctx, v1.PersistentVolumeClaimResizing)
	if stopPvcErr != nil {
		return stopPvcErr
	}
	for _, pvc := range stopPvcs {
		//deploy, err := c.getDeploy(ctx, &pvc)
		//if deploy.Spec.Replicas == 0 &&
		err := c.stopDeploy(ctx, &pvc)
		if err != nil {
			return err
		}
	}
	startPvc, err := c.getPVCListByConditionsType(ctx, v1.PersistentVolumeClaimFileSystemResizePending)
	if err != nil {
		return err
	}
	for _, pvc := range startPvc {
		err := c.StartDeploy(ctx, &pvc)
		if err != nil {
			return err
		}
	}
	return err
}

func (c *restarter) getPVCListByConditionsType(ctx context.Context, pvcType v1.PersistentVolumeClaimConditionType) ([]v1.PersistentVolumeClaim, error) {
	pvcs := make([]v1.PersistentVolumeClaim, 0)
	pvcList := v1.PersistentVolumeClaimList{}
	var opts []client.ListOption
	err := c.client.List(ctx, &pvcList, opts...)
	if err != nil {
		return pvcs, err
	}
	scNeedRestart := make(map[string]string, 0)
	scNeedRestart, err = c.getSc(ctx)
	if err != nil {
		return nil, err
	}
	for _, pvc := range pvcList.Items {
		if len(pvc.Status.Conditions) > 0 && pvc.Status.Conditions[0].Type == pvcType {
			if _, ok := scNeedRestart[*pvc.Spec.StorageClassName]; ok {
				pvcs = append(pvcs, pvc)
			}
		}
	}
	return pvcs, nil
}

func (c *restarter) getSc(ctx context.Context) (map[string]string, error) {
	scList := &storagev1.StorageClassList{}
	var opts []client.ListOption
	err := c.client.List(ctx, scList, opts...)
	if err != nil {
		return nil, err
	}
	scMap := make(map[string]string, 0)
	for _, sc := range scList.Items {
		if val, ok := sc.Annotations[SupportOnlineResize]; ok {
			SupportOnline, err := strconv.ParseBool(val)
			if err != nil {
				return nil, err
			}
			if !SupportOnline {
				if val, ok := sc.Annotations[AutoRestartEnabledKey]; ok {
					NeedRestart, err := strconv.ParseBool(val)
					if err != nil {
						return nil, err
					}
					if NeedRestart {
						scMap[sc.Name] = ""
					}
				}
			}
		}
	}
	return scMap, nil
}

func (c *restarter) stopDeploy(ctx context.Context, pvc *v1.PersistentVolumeClaim) error {
	var zero int32
	zero = 0
	deploy, getDeployErr := c.getDeploy(ctx, pvc)
	replicas := *deploy.Spec.Replicas
	if getDeployErr != nil {
		return getDeployErr
	}
	updateDeploy := deploy.DeepCopy()

	// add annotations
	updateDeploy.Annotations[RestartStopTime] = strconv.FormatInt(time.Now().Unix(), 10)
	updateDeploy.Annotations[ExpectReplicaNums] = strconv.Itoa(int(replicas))

	updateDeploy.Spec.Replicas = &zero
	var opts []client.UpdateOption
	updateErr := c.client.Update(ctx, updateDeploy, opts...)
	return updateErr
}

func (c *restarter) StartDeploy(ctx context.Context, pvc *v1.PersistentVolumeClaim) error {
	deploy, err := c.getDeploy(ctx, pvc)
	if err != nil {
		return err
	}
	updateDeploy := deploy.DeepCopy()
	expectReplicaNums, err := strconv.Atoi(deploy.Annotations[ExpectReplicaNums])
	if err != nil {
		return err
	}
	replicas := int32(expectReplicaNums)
	delete(updateDeploy.Annotations, RestartStopTime)
	delete(updateDeploy.Annotations, ExpectReplicaNums)
	updateDeploy.Spec.Replicas = &replicas
	var opts []client.UpdateOption
	err = c.client.Update(ctx, updateDeploy, opts...)
	return err
}

func (c *restarter) getDeploy(ctx context.Context, pvc *v1.PersistentVolumeClaim) (*deployV1.Deployment, error) {
	deployList := &deployV1.DeploymentList{}
	var opts []client.ListOption
	err := c.client.List(ctx, deployList, opts...)
	if err != nil {
		return nil, err
	}
	for _, deploy := range deployList.Items {
		if len(deploy.Spec.Template.Spec.Volumes) > 0 {
			for _, vol := range deploy.Spec.Template.Spec.Volumes {
				if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvc.Name {
					return &deploy, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("Cannot get deployment which pod mounted the pvc %s ", pvc.Name)
}
