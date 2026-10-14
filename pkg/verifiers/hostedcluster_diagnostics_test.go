package verifiers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	clienttesting "k8s.io/client-go/testing"
)

func diagnosticHC() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "hypershift.openshift.io/v1beta1", "kind": "HostedCluster",
		"metadata": map[string]interface{}{"name": "private-cluster", "namespace": "private-hc", "generation": int64(4)},
		"spec":     map[string]interface{}{"secret": "sensitive-sentinel"},
		"status": map[string]interface{}{"observedGeneration": int64(3), "endpoint": "sensitive-sentinel", "conditions": []interface{}{
			map[string]interface{}{"type": "Available", "status": "False", "observedGeneration": int64(3), "lastTransitionTime": "2026-01-02T03:04:05Z", "message": "sensitive-sentinel"},
			map[string]interface{}{"type": "Degraded", "status": "False"},
		}},
	}}
}

func TestHostedClusterDiagnosticsPopulatedAndSanitized(t *testing.T) {
	t.Parallel()
	hc := diagnosticHC()
	conditions, _, _ := unstructured.NestedSlice(hc.Object, "status", "conditions")
	conditions = append(conditions, map[string]interface{}{"type": "sensitive-sentinel", "status": "sensitive-sentinel", "lastTransitionTime": "sensitive-sentinel"})
	if err := unstructured.SetNestedSlice(hc.Object, conditions, "status", "conditions"); err != nil {
		t.Fatal(err)
	}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), hc)
	replicas := int32(3)
	kubeClient := fake.NewClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "redhat-operators-catalog", Namespace: "private-hcp", Generation: 4}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}, Status: appsv1.DeploymentStatus{
			ObservedGeneration: 3, Replicas: 3, ReadyReplicas: 1, AvailableReplicas: 1, UnavailableReplicas: 2,
			Conditions: []appsv1.DeploymentCondition{{Type: "Available", Status: "True", Message: "sensitive-sentinel", LastTransitionTime: metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))}},
		}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "sensitive-sentinel", Namespace: "private-hcp"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sensitive-sentinel", Namespace: "private-hcp", Labels: map[string]string{"olm.catalogSource": "redhat-operators", "customer": "sensitive-sentinel"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sensitive-sentinel", Image: "sensitive-sentinel", Env: []corev1.EnvVar{{Name: "TOKEN", Value: "sensitive-sentinel"}}}}},
			Status: corev1.PodStatus{Phase: "Running", PodIP: "192.0.2.1", Conditions: []corev1.PodCondition{{Type: "Ready", Status: "False", Message: "sensitive-sentinel"}},
				InitContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed", Message: "sensitive-sentinel"}}}},
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "sensitive-sentinel", RestartCount: 7, Image: "sensitive-sentinel", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "sensitive-sentinel"}}},
					{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "sensitive-sentinel", Message: "sensitive-sentinel"}}},
				},
			}},
	)
	output := collectHostedClusterHealthDiagnostics(context.Background(), dynamicClient, kubeClient, "private-hc", "private-cluster", "private-hcp")
	for _, forbidden := range []string{"sensitive-sentinel", "private-hc", "private-cluster", "192.0.2.1", "TOKEN", "message", "spec", "endpoint", "image"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("diagnostic leaked %q: %s", forbidden, output)
		}
	}
	var result healthDiagnostics
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatal(err)
	}
	if result.HostedCluster.Generation != 4 || *result.HostedCluster.Conditions[0].ObservedGeneration != 3 || result.HostedCluster.Conditions[0].LastTransitionTime != "2026-01-02T03:04:05Z" {
		t.Fatalf("missing HC generation/condition details: %s", output)
	}
	if len(result.Deployments) != 2 || result.Deployments[0].Ready != 1 || *result.Deployments[0].Desired != 3 || result.Deployments[0].Conditions[0].Status != "True" {
		t.Fatalf("missing deployment replica details despite Available=True: %s", output)
	}
	if len(result.CatalogPods) != 1 || result.CatalogPods[0].Containers[0].Reason != "CrashLoopBackOff" || result.CatalogPods[0].Containers[0].Restarts != 7 || result.CatalogPods[0].InitContainers[0].Reason != "Completed" || result.CatalogPods[0].Containers[1].Reason != "other" {
		t.Fatalf("missing catalog details: %s", output)
	}
	for _, action := range kubeClient.Actions() {
		if action.GetNamespace() != "private-hcp" || action.GetVerb() != "list" || (action.GetResource().Resource != "pods" && action.GetResource().Resource != "deployments") {
			t.Fatalf("unexpected access: %#v", action)
		}
		listAction := action.(clienttesting.ListAction)
		if action.(clienttesting.ListActionImpl).ListOptions.Limit != healthDiagnosticLimit {
			t.Fatal("missing server list limit")
		}
		selector, err := labels.Parse(catalogSelector)
		if err != nil {
			t.Fatal(err)
		}
		if action.GetResource().Resource == "pods" && listAction.GetListRestrictions().Labels.String() != selector.String() {
			t.Fatal("missing catalog selector")
		}
	}
}

func TestHostedClusterDiagnosticsMissingStatusAndPartialFailure(t *testing.T) {
	t.Parallel()
	hc := diagnosticHC()
	delete(hc.Object, "status")
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), hc)
	kubeClient := fake.NewClientset()
	kubeClient.PrependReactor("list", "deployments", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("sensitive-sentinel endpoint credentials")
	})
	var output string
	expected := VerifyHostedClusterHealthy(context.Background(), dynamicClient, "private-hc", "private-cluster")
	err := VerifyHostedClusterHealthyWithDiagnostics(context.Background(), dynamicClient, kubeClient, "private-hc", "private-cluster", "private-hcp", func(value string) { output = value })
	if err == nil || err.Error() != expected.Error() {
		t.Fatalf("lost original missing-status failure: %v", err)
	}
	if strings.Contains(output, "sensitive-sentinel") || !strings.Contains(output, `"deploymentsResult":"collection_failed"`) || !strings.Contains(output, `"catalogPodsResult":"collected"`) || !strings.Contains(output, `"result":"missing_status"`) {
		t.Fatalf("missing useful partial diagnostics: %s", output)
	}
}

type diagnosticTimeoutClient struct {
	kubernetes.Interface
	appsclient.AppsV1Interface
	appsclient.DeploymentInterface
	deadline time.Time
}

func (client *diagnosticTimeoutClient) AppsV1() appsclient.AppsV1Interface { return client }
func (client *diagnosticTimeoutClient) Deployments(string) appsclient.DeploymentInterface {
	return client
}
func (client *diagnosticTimeoutClient) List(ctx context.Context, _ metav1.ListOptions) (*appsv1.DeploymentList, error) {
	client.deadline, _ = ctx.Deadline()
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestHostedClusterDiagnosticsPreserveOriginalErrorOnTimeout(t *testing.T) {
	t.Parallel()
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	original := errors.New("original verification failure")
	calls := 0
	dynamicClient.PrependReactor("get", "hostedclusters", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, nil, original
		}
		return true, nil, errors.New("sensitive-sentinel")
	})
	kubeClient := &diagnosticTimeoutClient{Interface: fake.NewClientset()}
	started := time.Now()
	var output string
	err := VerifyHostedClusterHealthyWithDiagnostics(context.Background(), dynamicClient, kubeClient, "private-hc", "private-cluster", "private-hcp", func(value string) { output = value })
	if !errors.Is(err, original) || calls != 2 {
		t.Fatalf("original error replaced or verifier retried: %v, calls=%d", err, calls)
	}
	if kubeClient.deadline.IsZero() || kubeClient.deadline.Sub(started) > healthDiagnosticTimeout+time.Second || time.Since(started) > healthDiagnosticTimeout+2*time.Second {
		t.Fatal("collection deadline missing or exceeded")
	}
	if strings.Contains(output, "sensitive-sentinel") || !strings.Contains(output, `"deploymentsResult":"timeout_or_canceled"`) || !strings.Contains(output, `"hostedCluster":{"result":"collection_failed"`) {
		t.Fatalf("unexpected diagnostic: %s", output)
	}
}

func TestHostedClusterDiagnosticsPreserveHealthContract(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		available, degraded string
		healthy             bool
	}{
		{"True", "False", true}, {"False", "False", false}, {"True", "True", false}, {"Unknown", "False", false},
	} {
		t.Run(testCase.available+"/"+testCase.degraded, func(t *testing.T) {
			t.Parallel()
			hc := diagnosticHC()
			if err := unstructured.SetNestedSlice(hc.Object, []interface{}{
				map[string]interface{}{"type": "Available", "status": testCase.available},
				map[string]interface{}{"type": "Degraded", "status": testCase.degraded},
			}, "status", "conditions"); err != nil {
				t.Fatal(err)
			}
			dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), hc)
			kubeClient := fake.NewClientset()
			reports := 0
			err := VerifyHostedClusterHealthyWithDiagnostics(context.Background(), dynamicClient, kubeClient, "private-hc", "private-cluster", "private-hcp", func(string) { reports++ })
			if (err == nil) != testCase.healthy || (reports == 0) != testCase.healthy {
				t.Fatalf("contract changed: err=%v, reports=%d", err, reports)
			}
			if testCase.healthy && (len(dynamicClient.Actions()) != 1 || len(kubeClient.Actions()) != 0) {
				t.Fatal("healthy verification triggered collection")
			}
		})
	}
}

func TestHostedClusterDiagnosticsMalformedStatusAndCanceledParent(t *testing.T) {
	t.Parallel()
	hc := diagnosticHC()
	if err := unstructured.SetNestedField(hc.Object, "sensitive-sentinel", "status", "conditions"); err != nil {
		t.Fatal(err)
	}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), hc)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	kubeClient := &diagnosticTimeoutClient{Interface: fake.NewClientset()}
	started := time.Now()
	output := collectHostedClusterHealthDiagnostics(ctx, dynamicClient, kubeClient, "private-hc", "private-cluster", "private-hcp")
	if time.Since(started) > time.Second || strings.Contains(output, "sensitive-sentinel") || !strings.Contains(output, `"result":"invalid_status"`) || !strings.Contains(output, `"deploymentsResult":"timeout_or_canceled"`) {
		t.Fatalf("invalid status/cancellation not handled safely: %s", output)
	}
}

func TestHostedClusterDiagnosticsBounds(t *testing.T) {
	t.Parallel()
	hc := diagnosticHC()
	conditions := make([]interface{}, healthDiagnosticLimit+1)
	for index := range conditions {
		conditions[index] = map[string]interface{}{"type": "Available", "status": "False"}
	}
	if err := unstructured.SetNestedSlice(hc.Object, conditions, "status", "conditions"); err != nil {
		t.Fatal(err)
	}
	kubeClient := fake.NewClientset()
	kubeClient.PrependReactor("list", "deployments", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, &appsv1.DeploymentList{ListMeta: metav1.ListMeta{Continue: "sensitive-sentinel"}, Items: make([]appsv1.Deployment, healthDiagnosticLimit+1)}, nil
	})
	kubeClient.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		pods := make([]corev1.Pod, healthDiagnosticLimit+1)
		for index := range pods {
			pods[index].Labels = map[string]string{"olm.catalogSource": "redhat-operators"}
		}
		return true, &corev1.PodList{Items: pods}, nil
	})
	output := collectHostedClusterHealthDiagnostics(context.Background(), dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), hc), kubeClient, "private-hc", "private-cluster", "private-hcp")
	var result healthDiagnostics
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatal(err)
	}
	if !result.HostedCluster.Truncated || !result.DeploymentsTruncated || !result.CatalogPodsTruncated || len(result.HostedCluster.Conditions) != healthDiagnosticLimit || len(result.Deployments) != healthDiagnosticLimit || len(result.CatalogPods) != healthDiagnosticLimit || len(output) > 32*1024 || strings.Contains(output, "sensitive-sentinel") {
		t.Fatalf("diagnostics not bounded: %s", output)
	}
}
