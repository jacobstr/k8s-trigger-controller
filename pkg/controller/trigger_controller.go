package controller

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	appsv1client "k8s.io/client-go/kubernetes/typed/apps/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const (
	controllerAgentName = "trigger-controller"
	configMapPrefix     = "configMap#"
	secretPrefix        = "secret#"

	dataHashAnnotation = "trigger.k8s.io/data-hash"

	// triggerSecretsAnnotation is a deployment annotation that contains a comma separated list of secret names that this deployment should
	// be automatically triggered when the content of those secrets is changed.
	triggerSecretsAnnotation = "trigger.k8s.io/triggering-secrets"

	// triggerConfigMapsAnnotation is a deployment annotation that contains a comma separated list of configMaps names that this deployment should
	// be automatically triggered when the content of those configMaps is changed.
	triggerConfigMapsAnnotation = "trigger.k8s.io/triggering-configMaps"
)

var (
	// lastHashAnnotation is a deployment replicaset template annotation that contains last SHA256 hash of the secret or configMap
	lastHashAnnotation = func(kind, name string) string {
		return fmt.Sprintf("trigger.k8s.io/%s-%s-last-hash", kind, name)
	}
)

// TriggerController is the controller implementation for Foo resources
type TriggerController struct {
	client kubernetes.Interface

	configMapsLister v1.ConfigMapLister
	configMapsSynced cache.InformerSynced

	secretsLister v1.SecretLister
	secretsSynced cache.InformerSynced

	deploymentClient  appsv1client.DeploymentsGetter
	deploymentsSynced cache.InformerSynced

	deploymentsIndex cache.Indexer

	workqueue workqueue.RateLimitingInterface
	recorder  record.EventRecorder

	calculateDataHashFn func(interface{}) string
}

// NewTriggerController returns a new trigger controller
func NewTriggerController(
	kubeclientset kubernetes.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory) *TriggerController {

	configMapInformer := kubeInformerFactory.Core().V1().ConfigMaps()
	secretInformer := kubeInformerFactory.Core().V1().Secrets()
	deploymentInformer := kubeInformerFactory.Apps().V1().Deployments()

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &TriggerController{
		client:              kubeclientset,
		deploymentClient:    kubeclientset.AppsV1(),
		workqueue:           workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "data-version"),
		recorder:            recorder,
		secretsLister:       secretInformer.Lister(),
		configMapsLister:    configMapInformer.Lister(),
		calculateDataHashFn: calculateDataHash,
	}

	controller.configMapsSynced = configMapInformer.Informer().HasSynced
	controller.secretsSynced = secretInformer.Informer().HasSynced
	controller.deploymentsSynced = deploymentInformer.Informer().HasSynced

	deploymentInformer.Informer().AddIndexers(cache.Indexers{
		"configMap": indexByConfigMaps,
		"secret":    indexBySecrets,
	})
	controller.deploymentsIndex = deploymentInformer.Informer().GetIndexer()

	configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(new interface{}) {
			controller.enqueueConfigMap(nil, new)
		},
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueConfigMap(old, new)
		},
	})

	secretInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(new interface{}) {
			controller.enqueueSecret(nil, new)
		},
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueSecret(old, new)
		},
	})

	return controller
}

func indexByConfigMaps(obj interface{}) ([]string, error) {
	objMeta := meta.Accesor(obj)
	annotations := objMeta.GetAnnotations()
	if triggers, ok := annotations[triggerConfigMapsAnnotation]; ok {
		return sets.NewString(strings.Split(triggers, ",")...), nil
	}
	return []string{}, nil
}

func indexBySecrets(obj interface{}) ([]string, error) {
	objMeta := meta.Accesor(obj)
	annotations := objMeta.GetAnnotations()
	if triggers, ok := annotations[triggerSecretsAnnotation]; ok {
		return sets.NewString(strings.Split(triggers, ",")...), nil
	}
	return []string{}, nil
}

func (c *TriggerController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtimeutil.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	glog.Info("Starting trigger controller")

	// Wait for the caches to be synced before starting workers
	glog.Info("Waiting for informer caches to sync ...")
	if ok := cache.WaitForCacheSync(stopCh, c.configMapsSynced, c.secretsSynced, c.deploymentsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("Starting workers ...")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	glog.Info("Started workers")
	<-stopCh
	glog.Info("Shutting down workers ...")
	return nil
}

func (c *TriggerController) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *TriggerController) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var (
			key string
			ok  bool
		)
		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			runtimeutil.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if syncErr := c.syncHandler(key); syncErr != nil {
			return fmt.Errorf("error syncing '%s': %s", key, syncErr.Error())
		}
		c.workqueue.Forget(obj)
		return nil
	}(obj)
	if err != nil {
		runtimeutil.HandleError(err)
	}
	return true
}

func (c *TriggerController) enqueueObject(prefix string, old, new interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(new)
	if err != nil {
		runtimeutil.HandleError(err)
		return
	}
	c.workqueue.AddRateLimited(prefix + key)
}

func (c *TriggerController) enqueueConfigMap(old, new interface{}) {
	c.enqueueObject(configMapPrefix, old, new)
}

func (c *TriggerController) enqueueSecret(old, new interface{}) {
	c.enqueueObject(secretPrefix, old, new)
}

func (c *TriggerController) syncHandler(key string) error {
	parts := strings.Split(key, "#")
	if len(parts) != 2 {
		runtimeutil.HandleError(fmt.Errorf("unexpected resource key: %s", key))
		return nil
	}
	kind := parts[0]

	namespace, name, err := cache.SplitMetaNamespaceKey(parts[1])
	if err != nil {
		runtimeutil.HandleError(fmt.Errorf("invalid resource key: %s", parts[1]))
		return nil
	}

	var (
		obj interface{}
	)

	switch kind {
	case "configMap":
		obj, err = c.configMapsLister.ConfigMaps(namespace).Get(name)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
	case "secret":
		obj, err = c.secretsLister.Secrets(namespace).Get(name)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
	default:
		runtimeutil.HandleError(fmt.Errorf("invalid resource kind, only configMap and secret are allowed, got: %s", kind))
		return nil
	}

	// First update the data hashes into ConfigMap/Secret annotations.
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		runtimeutil.HandleError(err)
	}

	// Get all deployments that use the configMap or Secret
	toTrigger, err := c.deploymentsIndex.ByIndex(kind, objMeta.GetName())
	if err != nil {
		return err
	}

	// No triggers active for this secret/configMap
	if len(toTrigger) == 0 {
		// noisy: glog.V(5).Infof("%s %q is not triggering any deployment", kind, objMeta.GetName())
		return nil
	}

	newDataHash := c.calculateDataHashFn(obj)
	// The secret/configMap is empty
	// TODO: Should this trigger?
	if len(newDataHash) == 0 {
		return nil
	}
	oldDataHash := objMeta.GetAnnotations()[dataHashAnnotation]

	if newDataHash != oldDataHash {
		switch kind {
		case "configMap":
			objCopy := obj.(*corev1.ConfigMap).DeepCopy()
			if objCopy.Annotations == nil {
				objCopy.Annotations = map[string]string{}
			}
			objCopy.Annotations[dataHashAnnotation] = newDataHash
			glog.V(3).Infof("Updating configMap %s/%s with new data hash: %v", objCopy.Namespace, objCopy.Name, newDataHash)
			if _, err := c.client.CoreV1().ConfigMaps(objCopy.Namespace).Update(objCopy); err != nil {
				if errors.IsNotFound(err) {
					return nil
				}
				return err
			}
		case "secret":
			objCopy := obj.(*corev1.Secret).DeepCopy()
			if objCopy.Annotations == nil {
				objCopy.Annotations = map[string]string{}
			}
			objCopy.Annotations[dataHashAnnotation] = newDataHash
			glog.V(3).Infof("Updating secret %s/%s with new data hash: %v", objCopy.Namespace, objCopy.Name, newDataHash)
			if _, err := c.client.CoreV1().Secrets(objCopy.Namespace).Update(objCopy); err != nil {
				if errors.IsNotFound(err) {
					return nil
				}
				return err
			}
		}
	} else {
		glog.V(5).Infof("No change detected in hash for %s %s/%s", kind, objMeta.GetNamespace(), objMeta.GetName())
	}

	// Determine whether to trigger these deployments
	var triggerErrors []error
	for _, obj := range toTrigger {
		dMeta, err := meta.Accessor(obj)
		if err != nil {
			runtimeutil.HandleError(fmt.Errorf("failed to get accessor for %#v", err))
			continue
		}
		d, err := c.client.AppsV1().Deployments(dMeta.GetNamespace()).Get(dMeta.GetName(), meta_v1.GetOptions{})
		if err != nil {
			runtimeutil.HandleError(fmt.Errorf("failed to get deployment %s/%s: %v", dMeta.GetNamespace(), dMeta.GetName(), err))
		}
		glog.V(3).Infof("Processing deployment %s/%s that tracks %s %s ...", d.Namespace, d.Name, kind, objMeta.GetName())

		annotations := d.Spec.Template.Annotations
		if annotations == nil {
			annotations = map[string]string{}
		}
		triggerAnnotationKey := lastHashAnnotation(kind, objMeta.GetName())
		if hash, exists := annotations[triggerAnnotationKey]; exists && hash == newDataHash {
			glog.V(3).Infof("Deployment %s/%s already have latest %s %q", d.Namespace, d.Name, kind, objMeta.GetName())
			continue
		}

		glog.V(3).Infof("Deployment %s/%s has old %s %q and will rollout", d.Namespace, d.Name, kind, objMeta.GetName())
		annotations[triggerAnnotationKey] = newDataHash

		dCopy := d.DeepCopy()
		dCopy.Spec.Template.Annotations = annotations
		if _, err := c.client.AppsV1().Deployments(d.Namespace).Update(dCopy); err != nil {
			glog.Errorf("Failed to update deployment %s/%s: %v", d.Namespace, d.Name, err)
			triggerErrors = append(triggerErrors, err)
		}
	}

	if len(triggerErrors) != 0 {
		return errorsutil.NewAggregate(triggerErrors)
	}

	return nil
}
