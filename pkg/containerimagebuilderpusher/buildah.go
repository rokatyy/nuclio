/*
Copyright 2023 The Nuclio Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package containerimagebuilderpusher

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/nuclio/nuclio/pkg/cmdrunner"
	"github.com/nuclio/nuclio/pkg/common"
	"github.com/nuclio/nuclio/pkg/platform/kube/clients/kube"
	"github.com/nuclio/nuclio/pkg/platform/kube/utils"
	"github.com/nuclio/nuclio/pkg/processor/build/runtime"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	buildahAuthMountPath = "/auth"
)

type Buildah struct {
	kubeClientSet        kube.Client
	logger               logger.Logger
	builderConfiguration *ContainerBuilderConfiguration
	jobNameRegex         *regexp.Regexp
	cmdRunner            cmdrunner.CmdRunner
}

func NewBuildah(logger logger.Logger,
	kubeClientSet kube.Client,
	builderConfiguration *ContainerBuilderConfiguration) (*Buildah, error) {

	if builderConfiguration == nil {
		return nil, errors.New("Missing buildah builder configuration")
	}

	jobNameRegex := regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

	shellRunner, err := cmdrunner.NewShellRunner(logger)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create shell runner")
	}

	buildahBuilder := &Buildah{
		logger:               logger.GetChild("buildah"),
		kubeClientSet:        kubeClientSet,
		builderConfiguration: builderConfiguration,
		jobNameRegex:         jobNameRegex,
		cmdRunner:            shellRunner,
	}

	return buildahBuilder, nil
}

func (b *Buildah) GetKind() string {
	return "buildah"
}

func (b *Buildah) BuildAndPushContainerImage(ctx context.Context,
	buildOptions *BuildOptions,
	namespace string) error {

	if b.builderConfiguration.Kind != "buildah" {
		return errors.New("buildah builder is not enabled on this platform")
	}

	bundleFilename, assetPath, err := b.createContainerBuildBundle(ctx,
		buildOptions.Image,
		buildOptions.ContextDir,
		buildOptions.TempDir)
	if err != nil {
		return errors.Wrap(err, "Failed to create container build bundle")
	}

	defer os.Remove(assetPath) // nolint: errcheck

	jobSpec, err := b.compileJobSpec(ctx, namespace, buildOptions, bundleFilename)
	if err != nil {
		return errors.Wrap(err, "Failed to compile buildah job spec")
	}

	b.logger.DebugWithCtx(ctx,
		"Creating buildah job",
		"namespace", namespace,
		"jobSpec", jobSpec,
		"timeoutSeconds", buildOptions.BuildTimeoutSeconds,
	)
	job, err := b.kubeClientSet.CreateJob(ctx, namespace, jobSpec)
	if err != nil {
		return errors.Wrap(err, "Failed to publish buildah job")
	}

	defer time.AfterFunc(b.builderConfiguration.JobDeletionTimeout, func() {
		detachedCtx := context.WithoutCancel(ctx)
		if err := b.deleteJob(detachedCtx, namespace, job.Name); err != nil {
			b.logger.WarnWithCtx(ctx,
				"Failed to delete job",
				"err", err.Error())
		}
	})

	return b.waitForJobCompletion(ctx,
		namespace,
		job.Name,
		buildOptions.BuildTimeoutSeconds,
		buildOptions.ReadinessTimeoutSeconds,
		buildOptions.BuildLogger)
}

func (b *Buildah) GetOnbuildStages(onbuildArtifacts []runtime.Artifact) ([]string, error) {
	onbuildStages := make([]string, 0, len(onbuildArtifacts))
	stage := 0

	for _, artifact := range onbuildArtifacts {
		if artifact.ExternalImage {
			continue
		}

		stage++
		if len(artifact.Name) == 0 {
			artifact.Name = fmt.Sprintf("onbuildStage-%d", stage)
		}

		baseImage := fmt.Sprintf("FROM %s AS %s", artifact.Image, artifact.Name)
		onbuildDockerfileContents := fmt.Sprintf(`%s
ARG NUCLIO_LABEL
ARG NUCLIO_ARCH
`, baseImage)

		onbuildStages = append(onbuildStages, onbuildDockerfileContents)
	}

	return onbuildStages, nil
}

func (b *Buildah) GetDefaultRegistryCredentialsSecretName() string {
	return b.builderConfiguration.DefaultRegistryCredentialsSecretName
}

func (b *Buildah) TransformOnbuildArtifactPaths(onbuildArtifacts []runtime.Artifact) (map[string]string, error) {
	stagedArtifactPaths := make(map[string]string)
	for _, artifact := range onbuildArtifacts {
		for source, destination := range artifact.Paths {
			var transformedSource string
			if artifact.ExternalImage {
				transformedSource = fmt.Sprintf("--from=%s %s", artifact.Image, source)
			} else {
				transformedSource = fmt.Sprintf("--from=%s %s", artifact.Name, source)
			}
			stagedArtifactPaths[transformedSource] = destination
		}
	}
	return stagedArtifactPaths, nil
}

func (b *Buildah) GetBaseImageRegistry(registry string) string {
	return b.builderConfiguration.DefaultBaseRegistryURL
}

func (b *Buildah) GetRegistryKind() string {
	return b.builderConfiguration.RegistryKind
}

func (b *Buildah) GetOnbuildImageRegistry(registry string) string {
	return b.builderConfiguration.DefaultOnbuildRegistryURL
}

func (b *Buildah) compileBuildCommand(buildOptions *BuildOptions, fullImageName string) string {
	budArgs := []string{"buildah", "bud"}

	if buildOptions.NoCache {
		budArgs = append(budArgs, "--no-cache")
	} else {
		budArgs = append(budArgs, "--layers")
	}

	if buildOptions.DockerfileInfo != nil && buildOptions.DockerfileInfo.DockerfilePath != "" {
		budArgs = append(budArgs, "-f", buildOptions.DockerfileInfo.DockerfilePath)
	}

	budArgs = append(budArgs, "-t", fullImageName)

	if b.builderConfiguration.InsecurePullRegistry {
		budArgs = append(budArgs, "--tls-verify=false")
	}

	for buildArgName, buildArgValue := range buildOptions.BuildArgs {
		budArgs = append(budArgs, fmt.Sprintf("--build-arg=%s=%s", buildArgName, buildArgValue))
	}

	budArgs = append(budArgs, buildOptions.ContextDir)

	pushArgs := []string{"buildah", "push"}

	if len(buildOptions.SecretName) > 0 {
		pushArgs = append(pushArgs, "--authfile", buildahAuthMountPath+"/config.json")
	}

	if b.builderConfiguration.InsecurePushRegistry {
		pushArgs = append(pushArgs, "--tls-verify=false")
	}

	pushArgs = append(pushArgs, fullImageName)

	return fmt.Sprintf("%s && %s", strings.Join(budArgs, " "), strings.Join(pushArgs, " "))
}

func (b *Buildah) compileJobSpec(ctx context.Context,
	namespace string,
	buildOptions *BuildOptions,
	bundleFilename string) (*batchv1.Job, error) {

	completions := int32(1)
	backoffLimit := int32(0)

	tmpFolderVolumeMount := v1.VolumeMount{
		Name:      "tmp",
		MountPath: "/tmp",
	}
	jobName := b.compileJobName(ctx, buildOptions.Image)

	assetsURL := fmt.Sprintf("http://%s:8070/kaniko/%s", os.Getenv("NUCLIO_DASHBOARD_DEPLOYMENT_NAME"), bundleFilename)
	getAssetCommand := fmt.Sprintf("while true; do wget -T 5 -c %s -P %s && break; done", assetsURL, tmpFolderVolumeMount.MountPath)

	fullImageName := common.CompileImageName(buildOptions.RegistryURL, buildOptions.Image)
	buildCommand := b.compileBuildCommand(buildOptions, fullImageName)

	runAsNonRoot := true
	allowPrivilegeEscalation := false

	serviceAccount, err := b.enrichAndValidateServiceAccount(ctx, buildOptions, namespace)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to enrich and validate service account")
	}

	buildahJobSpec := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
		},
		Spec: batchv1.JobSpec{
			Completions:           &completions,
			ActiveDeadlineSeconds: &buildOptions.BuildTimeoutSeconds,
			BackoffLimit:          &backoffLimit,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: namespace,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:            "buildah-executor",
							Image:           b.builderConfiguration.BuildahImage,
							ImagePullPolicy: v1.PullPolicy(b.builderConfiguration.BuildahImagePullPolicy),
							Command:         []string{"/bin/sh"},
							Args:            []string{"-c", buildCommand},
							VolumeMounts:    []v1.VolumeMount{tmpFolderVolumeMount},
							SecurityContext: &v1.SecurityContext{
								RunAsNonRoot:             &runAsNonRoot,
								AllowPrivilegeEscalation: &allowPrivilegeEscalation,
								Capabilities: &v1.Capabilities{
									Drop: []v1.Capability{"ALL"},
									Add:  []v1.Capability{"SETUID", "SETGID"},
								},
							},
							Resources: buildOptions.Resources,
						},
					},
					InitContainers: []v1.Container{
						{
							Name:            "fetch-bundle",
							Image:           b.builderConfiguration.BusyBoxImage,
							ImagePullPolicy: v1.PullPolicy(b.builderConfiguration.BuildahImagePullPolicy),
							Command:         []string{"/bin/sh"},
							Args:            []string{"-c", getAssetCommand},
							VolumeMounts:    []v1.VolumeMount{tmpFolderVolumeMount},
							Resources:       buildOptions.Resources,
						},
						{
							Name:            "extract-bundle",
							Image:           b.builderConfiguration.BusyBoxImage,
							ImagePullPolicy: v1.PullPolicy(b.builderConfiguration.BuildahImagePullPolicy),
							Command: []string{
								"tar",
								"-xvf",
								fmt.Sprintf("%s/%s", tmpFolderVolumeMount.MountPath, bundleFilename),
								"-C",
								"/",
							},
							VolumeMounts: []v1.VolumeMount{tmpFolderVolumeMount},
							Resources:    buildOptions.Resources,
						},
					},
					Volumes: []v1.Volume{
						{
							Name: tmpFolderVolumeMount.Name,
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
					},
					RestartPolicy:      v1.RestartPolicyNever,
					NodeSelector:       buildOptions.NodeSelector,
					NodeName:           buildOptions.NodeName,
					Affinity:           buildOptions.Affinity,
					PriorityClassName:  buildOptions.PriorityClassName,
					Tolerations:        buildOptions.Tolerations,
					ServiceAccountName: serviceAccount,
				},
			},
		},
	}

	b.configureSecretVolumeMount(buildOptions, buildahJobSpec)
	return buildahJobSpec, nil
}

func (b *Buildah) configureSecretVolumeMount(buildOptions *BuildOptions, buildahJobSpec *batchv1.Job) {
	if b.matchECRUrl(buildOptions.RegistryURL) {
		b.configureECRInitContainerAndMount(buildOptions, buildahJobSpec)
	} else if len(buildOptions.SecretName) > 0 {
		buildahJobSpec.Spec.Template.Spec.Containers[0].VolumeMounts =
			append(buildahJobSpec.Spec.Template.Spec.Containers[0].VolumeMounts, v1.VolumeMount{
				Name:      "docker-config",
				MountPath: buildahAuthMountPath,
				ReadOnly:  true,
			})

		buildahJobSpec.Spec.Template.Spec.Volumes = append(buildahJobSpec.Spec.Template.Spec.Volumes, v1.Volume{
			Name: "docker-config",
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName: buildOptions.SecretName,
					Items: []v1.KeyToPath{
						{
							Key:  ".dockerconfigjson",
							Path: "config.json",
						},
					},
				},
			},
		})
	}
}

func (b *Buildah) configureECRInitContainerAndMount(buildOptions *BuildOptions, buildahJobSpec *batchv1.Job) {
	region := b.resolveAWSRegionFromECR(buildOptions.RegistryURL)
	createRepoTemplate := "aws ecr create-repository --repository-name %s --region %s || true"
	createMainRepo := fmt.Sprintf(createRepoTemplate, buildOptions.RepoName, region)
	createCacheRepo := fmt.Sprintf(createRepoTemplate,
		fmt.Sprintf("%s/cache", buildOptions.RepoName),
		region)
	createReposCommand := fmt.Sprintf("%s && %s", createMainRepo, createCacheRepo)

	initContainer := v1.Container{
		Name:            "create-repos",
		Image:           b.builderConfiguration.AWSCLIImage,
		ImagePullPolicy: v1.PullPolicy(b.builderConfiguration.BuildahImagePullPolicy),
		Command:         []string{"/bin/sh"},
		Args:            []string{"-c", createReposCommand},
	}

	if b.builderConfiguration.RegistryProviderSecretName != "" {
		initContainer.Env = []v1.EnvVar{
			{
				Name:  "AWS_SHARED_CREDENTIALS_FILE",
				Value: "/tmp/credentials",
			},
		}
		initContainer.VolumeMounts = []v1.VolumeMount{
			{
				Name:      b.builderConfiguration.RegistryProviderSecretName,
				MountPath: "/tmp",
			},
		}

		buildahJobSpec.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			buildahJobSpec.Spec.Template.Spec.Containers[0].VolumeMounts,
			v1.VolumeMount{
				Name:      b.builderConfiguration.RegistryProviderSecretName,
				MountPath: "/root/.aws/",
			})
		buildahJobSpec.Spec.Template.Spec.Volumes = append(buildahJobSpec.Spec.Template.Spec.Volumes,
			v1.Volume{
				Name: b.builderConfiguration.RegistryProviderSecretName,
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName: b.builderConfiguration.RegistryProviderSecretName,
					},
				},
			})
	} else {
		buildahJobSpec.Spec.Template.Spec.Containers[0].Env = append(buildahJobSpec.Spec.Template.Spec.Containers[0].Env,
			v1.EnvVar{
				Name:  "AWS_SDK_LOAD_CONFIG",
				Value: "true",
			})
	}
	buildahJobSpec.Spec.Template.Spec.InitContainers = append(buildahJobSpec.Spec.Template.Spec.InitContainers, initContainer)
}

func (b *Buildah) compileJobName(ctx context.Context, image string) string {
	functionName := strings.ReplaceAll(image, "/", "")
	functionName = strings.ReplaceAll(functionName, ":", "")
	functionName = strings.ReplaceAll(functionName, "-", "")
	randomSuffix := common.GenerateRandomString(10, common.SmallLettersAndNumbers)
	nuclioPrefix := "nuclio-"

	functionNameLimit := 63 - (len(b.builderConfiguration.JobPrefix) + len(randomSuffix) + len(nuclioPrefix) + 2)
	if len(functionName) > functionNameLimit {
		functionName = functionName[0:functionNameLimit]
	}

	jobName := fmt.Sprintf("%s%s.%s.%s", nuclioPrefix, b.builderConfiguration.JobPrefix, functionName, randomSuffix)

	if !b.jobNameRegex.MatchString(jobName) {
		b.logger.DebugWithCtx(ctx,
			"Job name does not match k8s regex. Won't use function name",
			"jobName", jobName)
		jobName = fmt.Sprintf("%s.%s", b.builderConfiguration.JobPrefix, randomSuffix)
	}

	return jobName
}

func (b *Buildah) waitForJobCompletion(ctx context.Context,
	namespace string,
	jobName string,
	buildTimeoutSeconds int64,
	readinessTimeoutSeconds int,
	buildLogger logger.Logger) error {

	b.logger.DebugWithCtx(ctx,
		"Waiting for job completion",
		"buildTimeoutSeconds", buildTimeoutSeconds,
		"readinessTimeoutSeconds", readinessTimeoutSeconds)
	timeout := time.Now().Add(time.Duration(buildTimeoutSeconds) * time.Second)

	if err := b.resolveFailFast(ctx, buildLogger, namespace, jobName, time.Duration(readinessTimeoutSeconds)*time.Second); err != nil {
		return errors.Wrap(err, "Buildah job failed to run")
	}

	for time.Now().Before(timeout) {
		runningJob, err := b.kubeClientSet.GetJob(ctx, namespace, jobName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				b.logger.WarnWithCtx(ctx,
					"Failed to pull buildah job status",
					"err", err.Error())
			}
			time.Sleep(1 * time.Second)
			continue
		}

		if runningJob.Status.Succeeded > 0 {
			jobLogs, err := b.getJobPodLogs(ctx, jobName, namespace)
			if err != nil {
				b.logger.DebugWithCtx(ctx,
					"Job was completed successfully but failed to retrieve job logs",
					"err", err.Error())
				return nil
			}
			b.logger.DebugWithCtx(ctx,
				"Job was completed successfully",
				"jobLogs", jobLogs)
			return nil
		}
		if runningJob.Status.Failed > 0 {
			jobPod, err := b.getJobPod(ctx, jobName, namespace, false)
			if err != nil {
				return errors.Wrap(err, "Failed to get job pod")
			}
			buildLogger.WarnWithCtx(ctx,
				"Build container image job has failed",
				"initContainerStatuses", jobPod.Status.InitContainerStatuses,
				"containerStatuses", jobPod.Status.ContainerStatuses,
				"conditions", jobPod.Status.Conditions,
				"reason", jobPod.Status.Reason,
				"message", jobPod.Status.Message,
				"phase", jobPod.Status.Phase,
				"jobName", jobName)

			jobLogs, err := b.getPodLogs(ctx, jobPod)
			if err != nil {
				buildLogger.WarnWithCtx(ctx,
					"Failed to get job logs", "err", err.Error())
				return errors.Wrap(err, "Failed to retrieve buildah job logs")
			}
			return errors.Errorf("Job failed. Job logs:\n%s", jobLogs)
		}

		b.logger.DebugWithCtx(ctx,
			"Waiting for job completion",
			"ttl", time.Until(timeout).String(),
			"jobName", jobName)
		time.Sleep(10 * time.Second)
	}

	jobPod, err := b.getJobPod(ctx, jobName, namespace, false)
	if err != nil {
		return errors.Wrap(err, "Job failed and was unable to get job pod")
	}

	b.logger.WarnWithCtx(ctx,
		"Build container image job has timed out",
		"initContainerStatuses", jobPod.Status.InitContainerStatuses,
		"containerStatuses", jobPod.Status.ContainerStatuses,
		"conditions", jobPod.Status.Conditions,
		"reason", jobPod.Status.Reason,
		"message", jobPod.Status.Message,
		"phase", jobPod.Status.Phase,
		"jobName", jobName)

	jobLogs, err := b.getPodLogs(ctx, jobPod)
	if err != nil {
		return errors.Wrap(err, "Job failed and was unable to retrieve job logs")
	}
	return errors.Errorf("Job has timed out. Job logs:\n%s", jobLogs)
}

func (b *Buildah) resolveFailFast(ctx context.Context,
	buildLogger logger.Logger,
	namespace,
	jobName string,
	readinessTimeout time.Duration) error {

	if readinessTimeout < 5*time.Minute {
		readinessTimeout = 5 * time.Minute
	}
	failFastTimeout := time.After(readinessTimeout)
	var lastError string

	for {
		select {
		case <-failFastTimeout:
			buildLogger.WarnWithCtx(ctx,
				"Buildah job was not completed in time",
				"jobName", jobName,
				"failFastTimeoutDuration", readinessTimeout.String())

			if lastError != "" {
				return errors.Errorf("Job was not completed in time, job name: %s. Error: %s", jobName, lastError)
			}
			return errors.Errorf("Job was not completed in time, job name: %s", jobName)
		default:
			jobPod, err := b.getJobPod(ctx, jobName, namespace, true)
			if err != nil {
				b.logger.WarnWithCtx(ctx,
					"Failed to get buildah job pod",
					"jobName", jobName,
					"err", err.Error())
				time.Sleep(5 * time.Second)
				continue
			}
			if jobPod.Status.Phase == v1.PodPending || jobPod.Status.Phase == v1.PodUnknown {
				if failure, failed := b.getLastPodWarningEvent(ctx, namespace, jobPod.Name); failed {
					errorMessage := fmt.Sprintf("%s event for Buildah pod %s. Message: %s",
						failure.Reason,
						jobPod.Name,
						failure.Message)
					if errorMessage != lastError {
						buildLogger.WarnWithCtx(ctx,
							"Buildah pod received a warning event",
							"eventReason", failure.Reason,
							"eventMessage", failure.Message,
							"podName", jobPod.Name)
						lastError = errorMessage
					}
				}
				time.Sleep(5 * time.Second)
				continue
			}
			return nil
		}
	}
}

func (b *Buildah) getJobPodLogs(ctx context.Context, jobName string, namespace string) (string, error) {
	jobPod, err := b.getJobPod(ctx, jobName, namespace, false)
	if err != nil {
		return "", errors.Wrap(err, "Failed to get job pod")
	}
	return b.getPodLogs(ctx, jobPod)
}

func (b *Buildah) getPodLogs(ctx context.Context, jobPod *v1.Pod) (string, error) {
	b.logger.DebugWithCtx(ctx,
		"Fetching pod logs",
		"name", jobPod.Name,
		"namespace", jobPod.Namespace)

	restReadCloser, err := b.kubeClientSet.StreamPodLogs(ctx, jobPod.Namespace, jobPod.Name, &v1.PodLogOptions{})
	if err != nil {
		return "", errors.Wrap(err, "Failed to get log read/closer")
	}
	defer restReadCloser.Close() // nolint: errcheck

	logContents, err := io.ReadAll(restReadCloser)
	if err != nil {
		return "", errors.Wrap(err, "Failed to read logs")
	}

	return b.prettifyLogContents(string(logContents)), nil
}

func (b *Buildah) getLastPodWarningEvent(ctx context.Context, namespace, podName string) (*v1.Event, bool) {
	events := b.getPodEvents(ctx, namespace, podName)
	if events == nil {
		return nil, false
	}
	for i := len(events.Items) - 1; i >= 0; i-- {
		if events.Items[i].Type == v1.EventTypeWarning {
			return &events.Items[i], true
		}
	}
	return nil, false
}

func (b *Buildah) getPodEvents(ctx context.Context, namespace, podName string) *v1.EventList {
	events, err := b.kubeClientSet.ListEvents(ctx, namespace, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.kind=Pod,involvedObject.name=%s", podName),
	})
	if err != nil {
		b.logger.WarnWithCtx(ctx,
			"Failed to list events for Buildah pod",
			"podName", podName,
			"err", err.Error())
		return nil
	}
	return events
}

func (b *Buildah) getJobPod(ctx context.Context, jobName, namespace string, quiet bool) (*v1.Pod, error) {
	if !quiet {
		b.logger.DebugWithCtx(ctx, "Getting job pods", "jobName", jobName)
	}
	jobPods, err := b.kubeClientSet.ListPods(ctx, namespace, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to list job's pods")
	}
	if len(jobPods.Items) == 0 {
		return nil, errors.New("No pods found for job")
	}
	if len(jobPods.Items) > 1 {
		return nil, errors.New("Got too many job pods")
	}
	return &jobPods.Items[0], nil
}

func (b *Buildah) prettifyLogContents(logContents string) string {
	scanner := bufio.NewScanner(strings.NewReader(logContents))
	lines := &[]string{}
	for scanner.Scan() {
		*lines = append(*lines, common.RemoveANSIColorsFromString(scanner.Text()))
	}
	return strings.Join(*lines, "\n")
}

func (b *Buildah) deleteJob(ctx context.Context, namespace string, jobName string) error {
	b.logger.DebugWithCtx(ctx, "Deleting job", "namespace", namespace, "job", jobName)
	propagationPolicy := metav1.DeletePropagationBackground
	if err := b.kubeClientSet.DeleteJob(ctx, namespace, jobName, metav1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}); err != nil {
		b.logger.WarnWithCtx(ctx,
			"Failed to delete buildah job",
			"namespace", namespace,
			"job", jobName,
			"error", err.Error(),
		)
		return errors.Wrap(err, "Failed to delete job")
	}
	b.logger.DebugWithCtx(ctx, "Successfully deleted job", "namespace", namespace, "job", jobName)
	return nil
}

func (b *Buildah) matchECRUrl(registryURL string) bool {
	return strings.Contains(registryURL, ".amazonaws.com") && strings.Contains(registryURL, ".ecr.")
}

func (b *Buildah) resolveAWSRegionFromECR(registryURL string) string {
	return strings.Split(registryURL, ".")[3]
}

func (b *Buildah) createContainerBuildBundle(ctx context.Context,
	image string,
	contextDir string,
	tempDir string) (string, string, error) {

	buildContainerBundleDir := path.Join(tempDir, "tar")
	if err := os.Mkdir(buildContainerBundleDir, 0744); err != nil {
		return "", "", errors.Wrapf(err, "Failed to create tar dir: %s", buildContainerBundleDir)
	}
	b.logger.DebugWithCtx(ctx, "Created tar dir", "dir", buildContainerBundleDir)

	tarFilename := fmt.Sprintf("%s.tar.gz", strings.ReplaceAll(image, "/", "_"))
	tarFilename = strings.ReplaceAll(tarFilename, ":", "_")
	tarFile, err := os.CreateTemp(buildContainerBundleDir, fmt.Sprintf("*-%s", tarFilename))
	if err != nil {
		return "", "", errors.Wrap(err, "Failed to create tar bundle")
	}
	tarFile.Chmod(0744) // nolint: errcheck
	tarFile.Close()     // nolint: errcheck

	b.logger.DebugWithCtx(ctx, "Compressing build bundle", "tarFilePath", tarFile.Name())
	if _, err := b.cmdRunner.Run(&cmdrunner.RunOptions{
		WorkingDir: &buildContainerBundleDir,
	}, "tar -zcvf %s %s", path.Base(tarFile.Name()), contextDir); err != nil {
		return "", "", errors.Wrapf(err, "Failed to compress build bundle")
	}

	buildDir := "/tmp/kaniko-builds"
	if err := os.MkdirAll(buildDir, 0755); err != nil {
		return "", "", errors.Wrapf(err, "Failed to ensure directory")
	}

	assetPath := path.Join(buildDir, path.Base(tarFile.Name()))
	b.logger.DebugWithCtx(ctx,
		"Creating symlink to bundle tar",
		"tarFileName", tarFile.Name(),
		"assetPath", assetPath)

	if err := os.Link(tarFile.Name(), assetPath); err != nil {
		return "", "", errors.Wrapf(err, "Failed to create symlink to build bundle")
	}

	return path.Base(tarFile.Name()), assetPath, nil
}

func (b *Buildah) enrichAndValidateServiceAccount(ctx context.Context, buildOptions *BuildOptions, namespace string) (string, error) {
	enrichedServiceAccount := b.enrichServiceAccountFromBuilderConfiguration(buildOptions)
	return utils.EnrichAndValidateServiceAccount(ctx,
		b.kubeClientSet,
		buildOptions.DefaultPlatformServiceAccount,
		buildOptions.ProjectSecretTemplate,
		buildOptions.ProjectSecretDefaultServiceAccountKey,
		buildOptions.ProjectSecretAllowedServiceAccountsKey,
		enrichedServiceAccount,
		buildOptions.ProjectName,
		namespace,
		true,
	)
}

func (b *Buildah) enrichServiceAccountFromBuilderConfiguration(buildOptions *BuildOptions) string {
	if buildOptions.BuilderServiceAccount != "" {
		return buildOptions.BuilderServiceAccount
	}
	if b.builderConfiguration.DefaultServiceAccount != "" {
		return b.builderConfiguration.DefaultServiceAccount
	}
	return buildOptions.FunctionServiceAccount
}
