pipeline {
  agent none
  stages {
    stage('Build Provisioner') {
      agent {
        node {
          label 'jenkins-buildkit'
        }

      }
      steps {
        sh '''buildctl-daemonless.sh build \\
	--frontend dockerfile.v0 \\
	--local context=. \\
	--local dockerfile=compute/compute_provisioner \\
	--opt build-arg:GIT_SHORT_COMMIT=${GIT_COMMIT:0:8} \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-provisioner:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-provisioner:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-provisioner:cache'''
      }
    }

  }
}