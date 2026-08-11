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

	"github.com/nuclio/nuclio/pkg/processor/build/runtime"

	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
	"k8s.io/api/core/v1"
)

type BuildahTestSuite struct {
	suite.Suite
	logger logger.Logger
}

func (suite *BuildahTestSuite) SetupSuite() {
	var err error
	suite.logger, err = nucliozap.NewNuclioZapTest("buildah-test")
	suite.Require().NoError(err)
}

func (suite *BuildahTestSuite) newBuildah(kind string) *Buildah {
	cfg := &ContainerBuilderConfiguration{
		Kind:                   kind,
		BuildahImage:           "quay.io/buildah/stable",
		BuildahImagePullPolicy: "IfNotPresent",
		BusyBoxImage:           "busybox:stable",
		JobPrefix:              "buildahjob",
		PushImagesRetries:      3,
	}
	b, err := NewBuildah(suite.logger, nil, cfg)
	suite.Require().NoError(err)
	return b
}

func (suite *BuildahTestSuite) newBuildOptions(secretName string, noCache bool) *BuildOptions {
	return &BuildOptions{
		Image:       "my-func:latest",
		ContextDir:  "/tmp/context",
		RegistryURL: "registry.example.com",
		DockerfileInfo: &runtime.ProcessorDockerfileInfo{
			DockerfilePath: "/tmp/context/Dockerfile",
		},
		SecretName:          secretName,
		NoCache:             noCache,
		BuildTimeoutSeconds: 600,
	}
}

// --- availability check ---

func (suite *BuildahTestSuite) TestBuildAvailabilityCheck_Disabled() {
	b := suite.newBuildah("kaniko")
	err := b.BuildAndPushContainerImage(context.Background(), suite.newBuildOptions("", false), "default")
	suite.Require().Error(err)
	suite.Contains(err.Error(), "buildah builder is not enabled on this platform")
}

func (suite *BuildahTestSuite) TestBuildAvailabilityCheck_Enabled() {
	b := suite.newBuildah("buildah")
	err := b.BuildAndPushContainerImage(context.Background(), suite.newBuildOptions("", false), "default")
	// The build will fail after the availability check (no real filesystem/k8s), but NOT with the "not enabled" error.
	if err != nil {
		suite.NotContains(err.Error(), "buildah builder is not enabled on this platform")
	}
}

// --- compileBuildCommand ---

func (suite *BuildahTestSuite) TestCompileBuildCommand_WithCache() {
	b := suite.newBuildah("buildah")
	opts := suite.newBuildOptions("mysecret", false)
	cmd := b.compileBuildCommand(opts, "registry.example.com/my-func:latest")

	suite.Contains(cmd, "--layers")
	suite.NotContains(cmd, "--no-cache")
	suite.Contains(cmd, "-f /tmp/context/Dockerfile")
	suite.Contains(cmd, "-t registry.example.com/my-func:latest")
	suite.Contains(cmd, "--authfile /auth/config.json")
	suite.Contains(cmd, "buildah bud")
	suite.Contains(cmd, "buildah push")
}

func (suite *BuildahTestSuite) TestCompileBuildCommand_NoCache() {
	b := suite.newBuildah("buildah")
	opts := suite.newBuildOptions("", true)
	cmd := b.compileBuildCommand(opts, "registry.example.com/my-func:latest")

	suite.Contains(cmd, "--no-cache")
	suite.NotContains(cmd, "--layers")
}

func (suite *BuildahTestSuite) TestCompileBuildCommand_NoAuth() {
	b := suite.newBuildah("buildah")
	opts := suite.newBuildOptions("", false)
	cmd := b.compileBuildCommand(opts, "registry.example.com/my-func:latest")

	suite.NotContains(cmd, "--authfile")
}

func (suite *BuildahTestSuite) TestCompileBuildCommand_InsecureRegistries() {
	b := suite.newBuildah("buildah")
	b.builderConfiguration.InsecurePullRegistry = true
	b.builderConfiguration.InsecurePushRegistry = true
	opts := suite.newBuildOptions("", false)
	cmd := b.compileBuildCommand(opts, "registry.example.com/my-func:latest")

	parts := strings.Split(cmd, "&&")
	suite.Require().Len(parts, 2)
	suite.Contains(parts[0], "--tls-verify=false")
	suite.Contains(parts[1], "--tls-verify=false")
}

func (suite *BuildahTestSuite) TestCompileBuildCommand_BuildArgs() {
	b := suite.newBuildah("buildah")
	opts := suite.newBuildOptions("", false)
	opts.BuildArgs = map[string]string{"FOO": "bar"}
	cmd := b.compileBuildCommand(opts, "registry.example.com/my-func:latest")

	suite.Contains(cmd, "--build-arg=FOO=bar")
}

// --- compileJobSpec ---

func (suite *BuildahTestSuite) TestCompileJobSpec_ContainerImage() {
	b := suite.newBuildah("buildah")
	spec, err := b.compileJobSpec(context.Background(), "default", suite.newBuildOptions("", false), "bundle.tar.gz")
	suite.Require().NoError(err)

	suite.Require().Len(spec.Spec.Template.Spec.Containers, 1)
	suite.Equal("quay.io/buildah/stable", spec.Spec.Template.Spec.Containers[0].Image)
	suite.Equal("buildah-executor", spec.Spec.Template.Spec.Containers[0].Name)
}

func (suite *BuildahTestSuite) TestCompileJobSpec_Command() {
	b := suite.newBuildah("buildah")
	spec, err := b.compileJobSpec(context.Background(), "default", suite.newBuildOptions("", false), "bundle.tar.gz")
	suite.Require().NoError(err)

	container := spec.Spec.Template.Spec.Containers[0]
	suite.Equal([]string{"/bin/sh"}, container.Command)
	suite.Require().Len(container.Args, 2)
	suite.Equal("-c", container.Args[0])
	suite.Contains(container.Args[1], "buildah bud")
	suite.Contains(container.Args[1], "buildah push")
}

func (suite *BuildahTestSuite) TestCompileJobSpec_RootlessSecurityContext() {
	b := suite.newBuildah("buildah")
	spec, err := b.compileJobSpec(context.Background(), "default", suite.newBuildOptions("", false), "bundle.tar.gz")
	suite.Require().NoError(err)

	sc := spec.Spec.Template.Spec.Containers[0].SecurityContext
	suite.Require().NotNil(sc)
	suite.Require().NotNil(sc.RunAsNonRoot)
	suite.True(*sc.RunAsNonRoot)
	suite.Require().NotNil(sc.AllowPrivilegeEscalation)
	suite.False(*sc.AllowPrivilegeEscalation)
	suite.Require().NotNil(sc.Capabilities)
	suite.Contains(sc.Capabilities.Drop, v1.Capability("ALL"))
	suite.Contains(sc.Capabilities.Add, v1.Capability("SETUID"))
	suite.Contains(sc.Capabilities.Add, v1.Capability("SETGID"))
}

func (suite *BuildahTestSuite) TestCompileJobSpec_InitContainers() {
	b := suite.newBuildah("buildah")
	spec, err := b.compileJobSpec(context.Background(), "default", suite.newBuildOptions("", false), "bundle.tar.gz")
	suite.Require().NoError(err)

	suite.Require().Len(spec.Spec.Template.Spec.InitContainers, 2)
	suite.Equal("fetch-bundle", spec.Spec.Template.Spec.InitContainers[0].Name)
	suite.Equal("extract-bundle", spec.Spec.Template.Spec.InitContainers[1].Name)
}

func (suite *BuildahTestSuite) TestCompileJobSpec_RegistryAuthMount() {
	b := suite.newBuildah("buildah")
	opts := suite.newBuildOptions("my-registry-secret", false)
	spec, err := b.compileJobSpec(context.Background(), "default", opts, "bundle.tar.gz")
	suite.Require().NoError(err)

	mainContainer := spec.Spec.Template.Spec.Containers[0]
	var authMount *v1.VolumeMount
	for i := range mainContainer.VolumeMounts {
		if mainContainer.VolumeMounts[i].Name == "docker-config" {
			authMount = &mainContainer.VolumeMounts[i]
			break
		}
	}
	suite.Require().NotNil(authMount, "expected docker-config volume mount")
	suite.Equal(buildahAuthMountPath, authMount.MountPath)
	suite.True(authMount.ReadOnly)

	var authVolume *v1.Volume
	for i := range spec.Spec.Template.Spec.Volumes {
		if spec.Spec.Template.Spec.Volumes[i].Name == "docker-config" {
			authVolume = &spec.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	suite.Require().NotNil(authVolume, "expected docker-config volume")
	suite.Require().NotNil(authVolume.VolumeSource.Secret)
	suite.Equal("my-registry-secret", authVolume.VolumeSource.Secret.SecretName)
	suite.Require().Len(authVolume.VolumeSource.Secret.Items, 1)
	suite.Equal(".dockerconfigjson", authVolume.VolumeSource.Secret.Items[0].Key)
	suite.Equal("config.json", authVolume.VolumeSource.Secret.Items[0].Path)
}

func (suite *BuildahTestSuite) TestCompileJobSpec_NoAuthMount_WhenNoSecret() {
	b := suite.newBuildah("buildah")
	opts := suite.newBuildOptions("", false)
	spec, err := b.compileJobSpec(context.Background(), "default", opts, "bundle.tar.gz")
	suite.Require().NoError(err)

	for _, vm := range spec.Spec.Template.Spec.Containers[0].VolumeMounts {
		suite.NotEqual("docker-config", vm.Name)
	}
}

func (suite *BuildahTestSuite) TestGetKind() {
	b := suite.newBuildah("buildah")
	suite.Equal("buildah", b.GetKind())
}

func TestBuildahTestSuite(t *testing.T) {
	suite.Run(t, new(BuildahTestSuite))
}
