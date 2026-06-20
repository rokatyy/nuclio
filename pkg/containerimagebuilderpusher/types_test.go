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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ContainerBuilderConfigurationTestSuite struct {
	suite.Suite
}

func (suite *ContainerBuilderConfigurationTestSuite) TestBuildahDefaults() {
	cfg, err := NewContainerBuilderConfiguration()
	suite.Require().NoError(err)

	suite.Equal("quay.io/buildah/stable:latest", cfg.BuildahImage)
	suite.Equal("IfNotPresent", cfg.BuildahImagePullPolicy)
	suite.Equal("overlay", cfg.BuildahStorageDriver)
	suite.Equal("chroot", cfg.BuildahIsolation)
	suite.Equal(false, cfg.BuildahPrivileged)
	suite.Equal("buildahjob", cfg.BuildahJobPrefix)
	suite.Equal(30*time.Minute, cfg.BuildahJobDeletionTimeout)
	suite.Equal(3, cfg.BuildahPushImagesRetries)
}

func (suite *ContainerBuilderConfigurationTestSuite) TestBuildahEnvVarOverrides() {
	suite.T().Setenv("NUCLIO_BUILDAH_CONTAINER_IMAGE", "quay.io/buildah/stable:v1.33.0")
	suite.T().Setenv("NUCLIO_BUILDAH_CONTAINER_IMAGE_PULL_POLICY", "Always")
	suite.T().Setenv("NUCLIO_BUILDAH_STORAGE_DRIVER", "vfs")
	suite.T().Setenv("NUCLIO_BUILDAH_ISOLATION", "rootless")
	suite.T().Setenv("NUCLIO_BUILDAH_PRIVILEGED", "true")
	suite.T().Setenv("NUCLIO_BUILDAH_JOB_NAME_PREFIX", "mybuildahjob")
	suite.T().Setenv("NUCLIO_BUILDAH_JOB_DELETION_TIMEOUT", "15m")
	suite.T().Setenv("NUCLIO_BUILDAH_PUSH_IMAGES_RETRIES", "5")

	cfg, err := NewContainerBuilderConfiguration()
	suite.Require().NoError(err)

	suite.Equal("quay.io/buildah/stable:v1.33.0", cfg.BuildahImage)
	suite.Equal("Always", cfg.BuildahImagePullPolicy)
	suite.Equal("vfs", cfg.BuildahStorageDriver)
	suite.Equal("rootless", cfg.BuildahIsolation)
	suite.Equal(true, cfg.BuildahPrivileged)
	suite.Equal("mybuildahjob", cfg.BuildahJobPrefix)
	suite.Equal(15*time.Minute, cfg.BuildahJobDeletionTimeout)
	suite.Equal(5, cfg.BuildahPushImagesRetries)
}

func (suite *ContainerBuilderConfigurationTestSuite) TestExistingKanikoFieldsUnchanged() {
	cfg, err := NewContainerBuilderConfiguration()
	suite.Require().NoError(err)

	// Verify existing Kaniko defaults remain intact
	suite.Equal("docker", cfg.Kind)
	suite.Equal("busybox:stable", cfg.BusyBoxImage)
	suite.Equal("gcr.io/kaniko-project/executor:v1.23.2", cfg.KanikoImage)
	suite.Equal("IfNotPresent", cfg.KanikoImagePullPolicy)
	suite.Equal("kanikojob", cfg.JobPrefix)
	suite.Equal(30*time.Minute, cfg.JobDeletionTimeout)
	suite.Equal(3, cfg.PushImagesRetries)
	suite.Equal(3, cfg.ImageFSExtractionRetries)
}

func TestContainerBuilderConfigurationTestSuite(t *testing.T) {
	suite.Run(t, new(ContainerBuilderConfigurationTestSuite))
}
