pipeline {
  agent none
  stages {
    stage('Build') {
      parallel {
        stage('provisioner') {
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
        --opt target=production \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-provisioner:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-provisioner:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-provisioner:cache'''
            }

          }
        }

        stage('tasks') {
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
        --opt target=production \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-tasks:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-tasks:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-tasks:cache'''
            }

          }
        }

        stage('wps') {
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
        --opt target=production \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-wps:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-wps:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-wps:cache'''
            }

          }
        }

        stage('thredds') {
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
        --opt target=production \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-thredds:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-thredds:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-thredds:cache'''
            }

          }
        }

      }
    }

    stage('Testing') {
      parallel {
        stage('tasks') {
          agent {
            node {
              label 'jenkins-buildkit'
            }

          }
          when {
            changeset '**/compute_tasks/**'
          }
          environment {
            MPC = credentials('myproxyclient')
          }
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
              sh '''buildctl-daemonless.sh build \\
	--frontend dockerfile.v0 \\
	--local context=. \\
	--local dockerfile=compute/compute_tasks\\
	--opt build-arg:GIT_SHORT_COMMIT=${GIT_COMMIT:0:8} \\
        --opt build-arg:MPC_HOST=esgf-node.llnl.gov \\
        --opt build-arg:MPC_USERNAME=${MPC_USR} \\
        --opt build-arg:MPC_PASSWORD=${MPC_PSW} \\
        --opt target=testresult \\
	--output type=local,dest=output \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-tasks:cache'''
              sh 'chown -R 10000:10000 output'
            }

            junit(testResults: 'output/unittesting.xml', healthScaleFactor: 1)
            cobertura(coberturaReportFile: 'output/coverage.xml', autoUpdateStability: true, autoUpdateHealth: true, classCoverageTargets: '80 80 80', conditionalCoverageTargets: '80 80 80', failNoReports: true, failUnhealthy: true, failUnstable: true, fileCoverageTargets: '80 80 80', lineCoverageTargets: '80 80 80', maxNumberOfBuilds: 2, methodCoverageTargets: '80 80 80', packageCoverageTargets: '80 80 80')
          }
        }

        stage('wps') {
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
        --opt target=testresult \\
	--output type=local,dest=output \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-wps:cache'''
              sh 'chown -R 10000:10000 output'
            }

            junit(testResults: 'output/unittest.xml', healthScaleFactor: 1)
            cobertura(coberturaReportFile: 'output/coverage.xml', autoUpdateHealth: true, maxNumberOfBuilds: 2, failNoReports: true, failUnhealthy: true, failUnstable: true, autoUpdateStability: true)
          }
        }

      }
    }

    stage('Deploy') {
      agent {
        node {
          label 'jenkins-helm'
        }

      }
      steps {
        container(name: 'helm', shell: '/bin/bash') {
          git(url: 'https://github.com/esgf-compute/charts', branch: 'devel')
          sh '''KUBECONFIG="--kubeconfig /jenkins-config/jenkins-config"

helm ${KUBECONFIG} init --client-only

helm repo add --ca-file /ssl/llnl.ca.pem stable https://kubernetes-charts.storage.googleapis.com/

helm ${KUBECONFIG} dependency update compute/

GIT_DIFF="$(git diff --name-only ${GIT_COMMIT} ${GIT_PREVIOUS_COMMIT})"

SET_FLAGS=""

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_provisioner/)" ]]
then
  SET_FLAGS="${SET_FLAGS} --set provisioner.imageTag=${GIT_COMMIT:0:8}"
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_wps/)" ]]
then
  SET_FLAGS="${SET_FLAGS} --set wps.imageTag=${GIT_COMMIT:0:8}"
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_tasks/)" ]]
then
  SET_FLAGS="${SET_FLAGS} --set celery.imageTag=${GIT_COMMIT:0:8}"
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /docker/thredds/)" ]]
then
  SET_FLAGS="${SET_FLAGS} --set thredds.imageTag=${GIT_COMMIT:0:8}"
fi

echo ${SET_FLAGS}

'''
        }

      }
    }

  }
}