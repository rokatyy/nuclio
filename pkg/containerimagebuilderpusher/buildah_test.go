//go:build test_unit

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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nuclio/nuclio/pkg/platform/kube/clients/kube"
	"github.com/nuclio/nuclio/pkg/processor/build/runtime"

	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type BuildahTestSuite struct {
	suite.Suite
	logger               logger.Logger
	builderConfiguration *ContainerBuilderConfiguration
	kubeClient           kube.Client
	ctx                  context.Context
}

func (suite *BuildahTestSuite) SetupSuite() {
	var err error
	suite.logger, err = nucliozap.NewNuclioZapTest("test")
	suite.Require().NoError(err)
	suite.ctx = context.Background()
	suite.kubeClient = kube.NewClientWithRetryFromClient(fake.NewSimpleClientset())
}

func (suite *BuildahTestSuite) SetupTest() {
	suite.builderConfiguration = &ContainerBuilderConfiguration{
		Kind:                   "buildah",
		BuildahImage:           "quay.io/buildah/stable:v1.36.0",
		BuildahImagePullPolicy: "IfNotPresent",
		BuildahPrivileged:      false,
		BusyBoxImage:           "busybox:stable",
		JobPrefix:              "buildahjob",
		JobDeletionTimeout:     30 * time.Minute,
		PushImagesRetries:      3,
		ImageFSExtractionRetries: 3,
	}
}

func (suite *BuildahTestSuite) newBuildah() *Buildah {
	b, err := NewBuildah(suite.logger, suite.kubeClient, suite.builderConfiguration)
	suite.Require().NoError(err)
	return b
}

func (suite *BuildahTestSuite) newBuildOptions(secretName string) *BuildOptions {
	return &BuildOptions{
		Image:               "test-registry/my-function:latest",
		ContextDir:          "/tmp/ctx",
		TempDir:             suite.T().TempDir(),
		DockerfileInfo:      &runtime.ProcessorDockerfileInfo{DockerfilePath: "/tmp/ctx/Dockerfile"},
		RegistryURL:         "test-registry",
		SecretName:          secretName,
		BuildTimeoutSeconds: 300,
		BuildArgs:           map[string]string{},
		BuildFlags:          map[string]bool{},
		BuildLogger:         suite.logger,
	}
}

// TestGetKind verifies the builder reports the correct kind.
func (suite *BuildahTestSuite) TestGetKind() {
	b := suite.newBuildah()
	suite.Equal("buildah", b.GetKind())
}

// TestNewBuildahSuccess verifies constructor succeeds with valid config.
func (suite *BuildahTestSuite) TestNewBuildahSuccess() {
	b, err := NewBuildah(suite.logger, suite.kubeClient, suite.builderConfiguration)
	suite.Require().NoError(err)
	suite.NotNil(b)
}

// TestNewBuildahNilConfig verifies constructor fails with nil configuration.
func (suite *BuildahTestSuite) TestNewBuildahNilConfig() {
	_, err := NewBuildah(suite.logger, suite.kubeClient, nil)
	suite.Require().Error(err)
	suite.Contains(err.Error(), "Missing buildah builder configuration")
}

// TestJobSpecImage verifies the job spec uses the configured Buildah image.
func (suite *BuildahTestSuite) TestJobSpecImage() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)
	suite.Equal("quay.io/buildah/stable:v1.36.0", jobSpec.Spec.Template.Spec.Containers[0].Image)
}

// TestJobSpecImagePullPolicy verifies the image pull policy is propagated.
func (suite *BuildahTestSuite) TestJobSpecImagePullPolicy() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)
	suite.Equal(corev1.PullIfNotPresent, jobSpec.Spec.Template.Spec.Containers[0].ImagePullPolicy)
}

// TestJobSpecContainerName verifies the main container has the expected name.
func (suite *BuildahTestSuite) TestJobSpecContainerName() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)
	suite.Equal("buildah-executor", jobSpec.Spec.Template.Spec.Containers[0].Name)
}

// TestJobSpecCommand verifies the job runs buildah bud and buildah push via shell.
func (suite *BuildahTestSuite) TestJobSpecCommand() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)

	container := jobSpec.Spec.Template.Spec.Containers[0]
	suite.Equal([]string{"/bin/sh"}, container.Command)
	suite.Require().Len(container.Args, 2)
	suite.Equal("-c", container.Args[0])
	cmd := container.Args[1]
	suite.Contains(cmd, "buildah bud")
	suite.Contains(cmd, "buildah push")
	suite.Contains(cmd, "--layers")
}

// TestJobSpecBuildAndPushDestination verifies image destination is in both bud and push commands.
func (suite *BuildahTestSuite) TestJobSpecBuildAndPushDestination() {
	b := suite.newBuildah()
	opts := suite.newBuildOptions("")
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", opts, "bundle.tar.gz")
	suite.Require().NoError(err)

	cmd := jobSpec.Spec.Template.Spec.Containers[0].Args[1]
	// destination should appear in both bud (--tag) and push
	suite.Equal(2, strings.Count(cmd, "test-registry/my-function:latest"),
		"expected destination image to appear in both bud and push commands")
}

// TestJobSpecNoCache verifies --no-cache is passed when NoCache=true.
func (suite *BuildahTestSuite) TestJobSpecNoCache() {
	b := suite.newBuildah()
	opts := suite.newBuildOptions("")
	opts.NoCache = true
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", opts, "bundle.tar.gz")
	suite.Require().NoError(err)
	suite.Contains(jobSpec.Spec.Template.Spec.Containers[0].Args[1], "--no-cache")
}

// TestJobSpecBuildArgs verifies build args are passed to buildah bud.
func (suite *BuildahTestSuite) TestJobSpecBuildArgs() {
	b := suite.newBuildah()
	opts := suite.newBuildOptions("")
	opts.BuildArgs = map[string]string{"MY_ARG": "my_value"}
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", opts, "bundle.tar.gz")
	suite.Require().NoError(err)
	suite.Contains(jobSpec.Spec.Template.Spec.Containers[0].Args[1], "--build-arg=MY_ARG=my_value")
}

// TestJobSpecInitContainers verifies fetch-bundle and extract-bundle init containers are present.
func (suite *BuildahTestSuite) TestJobSpecInitContainers() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)

	initContainerNames := make([]string, 0, len(jobSpec.Spec.Template.Spec.InitContainers))
	for _, ic := range jobSpec.Spec.Template.Spec.InitContainers {
		initContainerNames = append(initContainerNames, ic.Name)
	}
	suite.Contains(initContainerNames, "fetch-bundle")
	suite.Contains(initContainerNames, "extract-bundle")
}

// TestJobSpecTmpVolumePresent verifies the tmp emptyDir volume is present.
func (suite *BuildahTestSuite) TestJobSpecTmpVolumePresent() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)

	volumeNames := make([]string, 0, len(jobSpec.Spec.Template.Spec.Volumes))
	for _, v := range jobSpec.Spec.Template.Spec.Volumes {
		volumeNames = append(volumeNames, v.Name)
	}
	suite.Contains(volumeNames, "tmp")
}

// TestJobSpecRestartPolicy verifies restart policy is Never.
func (suite *BuildahTestSuite) TestJobSpecRestartPolicy() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)
	suite.Equal(corev1.RestartPolicyNever, jobSpec.Spec.Template.Spec.RestartPolicy)
}

// TestDefaultSecurityContextIsRootless verifies non-root security context is the default.
func (suite *BuildahTestSuite) TestDefaultSecurityContextIsRootless() {
	b := suite.newBuildah()
	sc := b.compileSecurityContext()

	suite.Require().NotNil(sc)
	suite.Require().NotNil(sc.RunAsNonRoot)
	suite.True(*sc.RunAsNonRoot, "expected RunAsNonRoot=true by default")
	suite.Require().NotNil(sc.AllowPrivilegeEscalation)
	suite.False(*sc.AllowPrivilegeEscalation, "expected AllowPrivilegeEscalation=false by default")
	suite.Nil(sc.Privileged, "expected Privileged not set in rootless mode")
}

// TestPrivilegedSecurityContext verifies privileged context when opt-in is set.
func (suite *BuildahTestSuite) TestPrivilegedSecurityContext() {
	suite.builderConfiguration.BuildahPrivileged = true
	b := suite.newBuildah()
	sc := b.compileSecurityContext()

	suite.Require().NotNil(sc)
	suite.Require().NotNil(sc.Privileged)
	suite.True(*sc.Privileged, "expected Privileged=true when BuildahPrivileged=true")
}

// TestJobSpecSecurityContextRootless verifies the job spec carries the rootless security context.
func (suite *BuildahTestSuite) TestJobSpecSecurityContextRootless() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)

	sc := jobSpec.Spec.Template.Spec.Containers[0].SecurityContext
	suite.Require().NotNil(sc)
	suite.Require().NotNil(sc.RunAsNonRoot)
	suite.True(*sc.RunAsNonRoot)
}

// TestRegistryAuthNoSecretWhenEmpty verifies no docker-config volume is added when SecretName is empty.
func (suite *BuildahTestSuite) TestRegistryAuthNoSecretWhenEmpty() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)

	for _, v := range jobSpec.Spec.Template.Spec.Volumes {
		suite.NotEqual("docker-config", v.Name, "expected no docker-config volume when SecretName is empty")
	}
}

// TestRegistryAuthSecretMountPresent verifies docker-config volume and DOCKER_CONFIG env are set
// when a SecretName is provided.
func (suite *BuildahTestSuite) TestRegistryAuthSecretMountPresent() {
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions("my-registry-secret"), "bundle.tar.gz")
	suite.Require().NoError(err)

	// check volume mount
	found := false
	for _, vm := range jobSpec.Spec.Template.Spec.Containers[0].VolumeMounts {
		if vm.Name == "docker-config" {
			found = true
			suite.Equal("/tmp/.docker", vm.MountPath)
			suite.True(vm.ReadOnly)
			break
		}
	}
	suite.True(found, "expected docker-config volume mount in main container")

	// check volume definition
	foundVol := false
	for _, v := range jobSpec.Spec.Template.Spec.Volumes {
		if v.Name == "docker-config" {
			foundVol = true
			suite.Require().NotNil(v.Secret)
			suite.Equal("my-registry-secret", v.Secret.SecretName)
			suite.Require().Len(v.Secret.Items, 1)
			suite.Equal(".dockerconfigjson", v.Secret.Items[0].Key)
			suite.Equal("config.json", v.Secret.Items[0].Path)
			break
		}
	}
	suite.True(foundVol, "expected docker-config volume in pod spec")

	// check DOCKER_CONFIG env var
	foundEnv := false
	for _, env := range jobSpec.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "DOCKER_CONFIG" {
			foundEnv = true
			suite.Equal("/tmp/.docker", env.Value)
			break
		}
	}
	suite.True(foundEnv, "expected DOCKER_CONFIG env var in main container")
}

// TestOnbuildStagesSkipsExternalImages verifies external images are skipped in onbuild stages.
func (suite *BuildahTestSuite) TestOnbuildStagesSkipsExternalImages() {
	b := suite.newBuildah()
	artifacts := []runtime.Artifact{
		{Image: "internal-image:v1", ExternalImage: false, Name: "stage1"},
		{Image: "nginx:latest", ExternalImage: true},
	}
	stages, err := b.GetOnbuildStages(artifacts)
	suite.Require().NoError(err)
	suite.Len(stages, 1)
	suite.Contains(stages[0], "FROM internal-image:v1 AS stage1")
}

// TestTransformOnbuildArtifactPathsInternal verifies internal artifact path transformation.
func (suite *BuildahTestSuite) TestTransformOnbuildArtifactPathsInternal() {
	b := suite.newBuildah()
	artifacts := []runtime.Artifact{
		{
			Image:         "internal-image:v1",
			Name:          "myStage",
			ExternalImage: false,
			Paths:         map[string]string{"/src/bin": "/app/bin"},
		},
	}
	paths, err := b.TransformOnbuildArtifactPaths(artifacts)
	suite.Require().NoError(err)
	suite.Len(paths, 1)
	for src, dst := range paths {
		suite.Equal("--from=myStage /src/bin", src)
		suite.Equal("/app/bin", dst)
	}
}

// TestGetDefaultRegistryCredentialsSecretName verifies the credentials secret name is returned from config.
func (suite *BuildahTestSuite) TestGetDefaultRegistryCredentialsSecretName() {
	suite.builderConfiguration.DefaultRegistryCredentialsSecretName = "my-credentials"
	b := suite.newBuildah()
	suite.Equal("my-credentials", b.GetDefaultRegistryCredentialsSecretName())
}

// TestGetBaseImageRegistry verifies the base image registry is returned from config.
func (suite *BuildahTestSuite) TestGetBaseImageRegistry() {
	suite.builderConfiguration.DefaultBaseRegistryURL = "my-registry.io"
	b := suite.newBuildah()
	suite.Equal("my-registry.io", b.GetBaseImageRegistry(""))
}

// TestGetRegistryKind verifies the registry kind is returned from config.
func (suite *BuildahTestSuite) TestGetRegistryKind() {
	suite.builderConfiguration.RegistryKind = "onCluster"
	b := suite.newBuildah()
	suite.Equal("onCluster", b.GetRegistryKind())
}

// TestGetOnbuildImageRegistry verifies the onbuild image registry is returned from config.
func (suite *BuildahTestSuite) TestGetOnbuildImageRegistry() {
	suite.builderConfiguration.DefaultOnbuildRegistryURL = "quay.io"
	b := suite.newBuildah()
	suite.Equal("quay.io", b.GetOnbuildImageRegistry(""))
}

// TestInsecurePushRegistryFlag verifies --tls-verify=false is added to both bud and push when InsecurePushRegistry=true.
func (suite *BuildahTestSuite) TestInsecurePushRegistryFlag() {
	suite.builderConfiguration.InsecurePushRegistry = true
	b := suite.newBuildah()
	jobSpec, err := b.compileJobSpec(suite.ctx, "default", suite.newBuildOptions(""), "bundle.tar.gz")
	suite.Require().NoError(err)
	cmd := jobSpec.Spec.Template.Spec.Containers[0].Args[1]
	suite.GreaterOrEqual(strings.Count(cmd, "--tls-verify=false"), 2,
		"expected --tls-verify=false in both bud and push commands")
}

func TestBuildahSuite(t *testing.T) {
	suite.Run(t, new(BuildahTestSuite))
}
