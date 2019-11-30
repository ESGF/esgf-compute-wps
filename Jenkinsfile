pipeline {
  agent none
  stages {
    stage('Build Provisioner') {
      parallel {
        stage('Build Provisioner') {
          agent {
            node {
              label 'jenkins-buildkit'
            }

          }
          when {
            changeset '**/compute_provisioner/**'
          }
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
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

        stage('Build Task') {
          agent {
            node {
              label 'jenkins-buildkit'
            }

          }
          when {
            changeset '**/compute_tasks/**'
          }
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
              sh '''buildctl-daemonless.sh build \\
	--frontend dockerfile.v0 \\
	--local context=. \\
	--local dockerfile=compute/compute_tasks \\
	--opt build-arg:GIT_SHORT_COMMIT=${GIT_COMMIT:0:8} \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-tasks:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-tasks:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-tasks:cache'''
            }

          }
        }

        stage('Build WPS') {
          agent {
            node {
              label 'jenkins-buildkit'
            }

          }
          when {
            changeset '**/compute_wps/**'
          }
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
              sh '''buildctl-daemonless.sh build \\
	--frontend dockerfile.v0 \\
	--local context=. \\
	--local dockerfile=compute/compute_wps \\
	--opt build-arg:GIT_SHORT_COMMIT=${GIT_COMMIT:0:8} \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-wps:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-wps:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-wps:cache'''
            }

          }
        }

        stage('Build Thredds') {
          agent {
            node {
              label 'jenkins-buildkit'
            }

          }
          when {
            changeset '**/docker/thredds/**'
          }
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
              sh '''buildctl-daemonless.sh build \\
	--frontend dockerfile.v0 \\
	--local context=. \\
	--local dockerfile=docker/thredds/ \\
	--opt build-arg:GIT_SHORT_COMMIT=${GIT_COMMIT:0:8} \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-thredds:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-thredds:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-thredds:cache'''
            }

          }
        }

      }
    }

    stage('Testing') {
      agent {
        node {
          label 'jenkins-buildkit'
        }

      }
      steps {
        container(name: 'buildkit', shell: '/bin/sh') {
          sh '''buildctl-daemonless.sh build \\
	--frontend dockerfile.v0 \\
	--local context=. \\
	--local dockerfile=compute/compute_tasks\\
	--opt build-arg:GIT_SHORT_COMMIT=${GIT_COMMIT:0:8} \\
        --opt target=testresult \\
	--output type=local,dest=output \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-tasks:cache

chown -R 10000:10000 output'''
        }

        sh '''ls -la
whoami
id -u
id -g'''
        junit 'unittesting.xml'
      }
    }

  }
}