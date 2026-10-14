package verifiers

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const (
	healthDiagnosticTimeout = 5 * time.Second
	healthDiagnosticLimit   = 10
	catalogSelector         = "olm.catalogSource in (certified-operators,community-operators,redhat-operators,redhat-marketplace)"
)

type healthConditionDiagnostic struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	ObservedGeneration *int64 `json:"observedGeneration,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type hostedClusterDiagnostic struct {
	Result             string                      `json:"result"`
	Generation         int64                       `json:"generation"`
	ObservedGeneration *int64                      `json:"observedGeneration,omitempty"`
	Conditions         []healthConditionDiagnostic `json:"conditions"`
	Truncated          bool                        `json:"truncated"`
}

type deploymentDiagnostic struct {
	Workload           string                      `json:"workload"`
	Generation         int64                       `json:"generation"`
	ObservedGeneration int64                       `json:"observedGeneration"`
	Desired            *int32                      `json:"desired"`
	Replicas           int32                       `json:"replicas"`
	Updated            int32                       `json:"updated"`
	Ready              int32                       `json:"ready"`
	Available          int32                       `json:"available"`
	Unavailable        int32                       `json:"unavailable"`
	Conditions         []healthConditionDiagnostic `json:"conditions"`
	Truncated          bool                        `json:"truncated"`
}

type containerDiagnostic struct {
	Ready    bool   `json:"ready"`
	Restarts int32  `json:"restarts"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
}

type catalogPodDiagnostic struct {
	Catalog        string                      `json:"catalog"`
	Phase          string                      `json:"phase"`
	Conditions     []healthConditionDiagnostic `json:"conditions"`
	InitContainers []containerDiagnostic       `json:"initContainers"`
	Containers     []containerDiagnostic       `json:"containers"`
	Truncated      bool                        `json:"truncated"`
}

type healthDiagnostics struct {
	HostedCluster        hostedClusterDiagnostic `json:"hostedCluster"`
	DeploymentsResult    string                  `json:"deploymentsResult"`
	Deployments          []deploymentDiagnostic  `json:"deployments"`
	DeploymentsTruncated bool                    `json:"deploymentsTruncated"`
	CatalogPodsResult    string                  `json:"catalogPodsResult"`
	CatalogPods          []catalogPodDiagnostic  `json:"catalogPods"`
	CatalogPodsTruncated bool                    `json:"catalogPodsTruncated"`
}

func VerifyHostedClusterHealthyWithDiagnostics(ctx context.Context, dynamicClient dynamic.Interface, kubeClient kubernetes.Interface, namespace, name, hcpNamespace string, report func(string)) error {
	err := VerifyHostedClusterHealthy(ctx, dynamicClient, namespace, name)
	if err != nil {
		report(collectHostedClusterHealthDiagnostics(ctx, dynamicClient, kubeClient, namespace, name, hcpNamespace))
	}
	return err
}

func collectHostedClusterHealthDiagnostics(ctx context.Context, dynamicClient dynamic.Interface, kubeClient kubernetes.Interface, namespace, name, hcpNamespace string) string {
	ctx, cancel := context.WithTimeout(ctx, healthDiagnosticTimeout)
	defer cancel()
	diagnostic := healthDiagnostics{}
	hc, err := dynamicClient.Resource(hostedClusterGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	diagnostic.HostedCluster.Result = diagnosticResult(err)
	if err == nil {
		diagnostic.HostedCluster.Generation = hc.GetGeneration()
		if observed, found, _ := unstructured.NestedInt64(hc.Object, "status", "observedGeneration"); found {
			diagnostic.HostedCluster.ObservedGeneration = &observed
		}
		conditions, found, conditionErr := unstructured.NestedSlice(hc.Object, "status", "conditions")
		if conditionErr != nil {
			diagnostic.HostedCluster.Result = "invalid_status"
		} else if !found || len(conditions) == 0 {
			diagnostic.HostedCluster.Result = "missing_status"
		}
		diagnostic.HostedCluster.Truncated = len(conditions) > healthDiagnosticLimit
		for _, condition := range conditions[:min(len(conditions), healthDiagnosticLimit)] {
			fields, ok := condition.(map[string]interface{})
			if !ok {
				diagnostic.HostedCluster.Result = "invalid_status"
				continue
			}
			projected := healthConditionDiagnostic{
				Type:               diagnosticEnum(stringField(fields, "type"), "Available", "Degraded", "Progressing", "Upgradeable", "ReconciliationActive", "ValidConfiguration", "SupportedHostedCluster", "ClusterVersionSucceeding"),
				Status:             diagnosticEnum(stringField(fields, "status"), "True", "False", "Unknown"),
				LastTransitionTime: diagnosticTime(stringField(fields, "lastTransitionTime")),
			}
			if observed, ok := fields["observedGeneration"].(int64); ok {
				projected.ObservedGeneration = &observed
			}
			diagnostic.HostedCluster.Conditions = append(diagnostic.HostedCluster.Conditions, projected)
		}
	}
	deployments, err := kubeClient.AppsV1().Deployments(hcpNamespace).List(ctx, metav1.ListOptions{Limit: healthDiagnosticLimit})
	diagnostic.DeploymentsResult = diagnosticResult(err)
	if err == nil {
		diagnostic.DeploymentsTruncated = deployments.Continue != "" || len(deployments.Items) > healthDiagnosticLimit
		for _, deployment := range deployments.Items[:min(len(deployments.Items), healthDiagnosticLimit)] {
			projected := deploymentDiagnostic{
				Workload:   diagnosticEnum(deployment.Name, "certified-operators-catalog", "community-operators-catalog", "redhat-operators-catalog", "redhat-marketplace-catalog", "kube-apiserver", "kube-controller-manager", "kube-scheduler", "cluster-version-operator", "control-plane-operator", "catalog-operator", "olm-operator"),
				Generation: deployment.Generation, ObservedGeneration: deployment.Status.ObservedGeneration,
				Desired: deployment.Spec.Replicas, Replicas: deployment.Status.Replicas, Updated: deployment.Status.UpdatedReplicas,
				Ready: deployment.Status.ReadyReplicas, Available: deployment.Status.AvailableReplicas, Unavailable: deployment.Status.UnavailableReplicas,
				Truncated: len(deployment.Status.Conditions) > healthDiagnosticLimit,
			}
			for _, condition := range deployment.Status.Conditions[:min(len(deployment.Status.Conditions), healthDiagnosticLimit)] {
				projected.Conditions = append(projected.Conditions, healthConditionDiagnostic{
					Type:   diagnosticEnum(string(condition.Type), "Available", "Progressing", "ReplicaFailure"),
					Status: diagnosticEnum(string(condition.Status), "True", "False", "Unknown"), LastTransitionTime: diagnosticTime(condition.LastTransitionTime.Format(time.RFC3339)),
				})
			}
			diagnostic.Deployments = append(diagnostic.Deployments, projected)
		}
	}
	pods, err := kubeClient.CoreV1().Pods(hcpNamespace).List(ctx, metav1.ListOptions{Limit: healthDiagnosticLimit, LabelSelector: catalogSelector})
	diagnostic.CatalogPodsResult = diagnosticResult(err)
	if err == nil {
		diagnostic.CatalogPodsTruncated = pods.Continue != "" || len(pods.Items) > healthDiagnosticLimit
		for _, pod := range pods.Items[:min(len(pods.Items), healthDiagnosticLimit)] {
			projected := catalogPodDiagnostic{
				Catalog:        diagnosticEnum(pod.Labels["olm.catalogSource"], "certified-operators", "community-operators", "redhat-operators", "redhat-marketplace"),
				Phase:          diagnosticEnum(string(pod.Status.Phase), "Pending", "Running", "Succeeded", "Failed", "Unknown"),
				InitContainers: diagnosticContainers(pod.Status.InitContainerStatuses), Containers: diagnosticContainers(pod.Status.ContainerStatuses),
				Truncated: len(pod.Status.Conditions) > healthDiagnosticLimit || len(pod.Status.InitContainerStatuses) > healthDiagnosticLimit || len(pod.Status.ContainerStatuses) > healthDiagnosticLimit,
			}
			for _, condition := range pod.Status.Conditions[:min(len(pod.Status.Conditions), healthDiagnosticLimit)] {
				projected.Conditions = append(projected.Conditions, healthConditionDiagnostic{
					Type:   diagnosticEnum(string(condition.Type), "Ready", "ContainersReady", "Initialized", "PodScheduled", "PodReadyToStartContainers"),
					Status: diagnosticEnum(string(condition.Status), "True", "False", "Unknown"),
				})
			}
			diagnostic.CatalogPods = append(diagnostic.CatalogPods, projected)
		}
	}
	output, err := json.Marshal(diagnostic)
	if err != nil || len(output) > 32*1024 {
		return `{"diagnosticsResult":"output_limit"}`
	}
	return string(output)
}

func diagnosticContainers(statuses []corev1.ContainerStatus) []containerDiagnostic {
	var result []containerDiagnostic
	for _, status := range statuses[:min(len(statuses), healthDiagnosticLimit)] {
		projected := containerDiagnostic{Ready: status.Ready, Restarts: status.RestartCount, State: "unknown"}
		if status.State.Running != nil {
			projected.State = "running"
		} else if status.State.Waiting != nil {
			projected.State = "waiting"
			projected.Reason = diagnosticContainerReason(status.State.Waiting.Reason)
		} else if status.State.Terminated != nil {
			projected.State = "terminated"
			projected.Reason = diagnosticContainerReason(status.State.Terminated.Reason)
		}
		result = append(result, projected)
	}
	return result
}

func diagnosticContainerReason(reason string) string {
	return diagnosticEnum(reason, "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "ContainerCreating", "PodInitializing", "CreateContainerConfigError", "CreateContainerError", "RunContainerError", "Error", "Completed", "OOMKilled", "ContainerCannotRun", "StartError")
}

func diagnosticEnum(value string, allowed ...string) string {
	if slices.Contains(allowed, value) {
		return value
	}
	return "other"
}

func diagnosticTime(value string) string {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return ""
	}
	return parsed.UTC().Format(time.RFC3339)
}

func stringField(fields map[string]interface{}, key string) string {
	value, _ := fields[key].(string)
	return value
}

func diagnosticResult(err error) string {
	switch {
	case err == nil:
		return "collected"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), apierrors.IsTimeout(err), apierrors.IsServerTimeout(err):
		return "timeout_or_canceled"
	case apierrors.IsForbidden(err):
		return "forbidden"
	case apierrors.IsNotFound(err):
		return "not_found"
	default:
		return "collection_failed"
	}
}
