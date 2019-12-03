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
            anyOf {
              expression {
                return params.FORCE_PROVISIONER
              }

              changeset '**/compute_provisioner/**'
            }

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
            anyOf {
              expression {
                return params.FORCE_TASKS
              }

              changeset '**/compute_tasks/**'
            }

          }
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
              sh '''buildctl-daemonless.sh build \\
	--frontend dockerfile.v0 \\
	--local context=. \\
	--local dockerfile=compute/compute_tasks \\
	--opt build-arg:GIT_SHORT_COMMIT=${GIT_COMMIT:0:8} \\
        --opt target=production \\
	--output type=image,name=${OUTPUT_REGISTRY}/compute-celery:${GIT_COMMIT:0:8},push=true \\
	--export-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-celery:cache \\
	--import-cache type=registry,ref=${OUTPUT_REGISTRY}/compute-celery:cache'''
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
            anyOf {
              expression {
                return params.FORCE_WPS
              }

              changeset '**/compute_wps/**'
            }

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
            anyOf {
              expression {
                return params.FORCE_THREDDS
              }

              changeset '**/docker/thredds/**'
            }

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
            anyOf {
              changeset '**/compute_tasks/**'
            }

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
            anyOf {
              changeset '**/compute_wps/**'
            }

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
      environment {
        GH = credentials('ae3dd8dc-817a-409b-90b9-6459fb524afc')
      }
      steps {
        container(name: 'helm', shell: '/bin/bash') {
          git(url: 'https://github.com/esgf-compute/charts', branch: 'devel')
          sh '''#! /bin/bash

KUBECONFIG="--kubeconfig /jenkins-config/jenkins-config"

helm ${KUBECONFIG} init --client-only

helm repo add --ca-file /ssl/llnl.ca.pem stable https://kubernetes-charts.storage.googleapis.com/

helm ${KUBECONFIG} dependency update compute/

conda install -c conda-forge ruamel.yaml

GIT_DIFF="$(git diff --name-only ${GIT_COMMIT} ${GIT_PREVIOUS_COMMIT})"

SET_FLAGS=""

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_provisioner/)" ]] || [[ "${FORCE_PROVISIONER}" == "true" ]]
then
  SET_FLAGS="${SET_FLAGS} --set provisioner.imageTag=${GIT_COMMIT:0:8}"

  python scripts/update_config.py configs/development.yaml provisioner ${GIT_COMMIT:0:8}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_wps/)" ]] || [[ "${FORCE_WPS}" == "true" ]]
then
  SET_FLAGS="${SET_FLAGS} --set wps.imageTag=${GIT_COMMIT:0:8}"

  python scripts/update_config.py configs/development.yaml wps ${GIT_COMMIT:0:8}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_tasks/)" ]] || [[ "${FORCE_TASKS}" == "true" ]]
then
  SET_FLAGS="${SET_FLAGS} --set celery.imageTag=${GIT_COMMIT:0:8}"

  python scripts/update_config.py configs/development.yaml celery ${GIT_COMMIT:0:8}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /docker/thredds/)" ]] || [[ "${FORCE_THREDDS}" == "true" ]]
then
  SET_FLAGS="${SET_FLAGS} --set thredds.imageTag=${GIT_COMMIT:0:8}"

  python scripts/update_config.py configs/development.yaml thredds ${GIT_COMMIT:0:8}
fi

helm ${KUBECONFIG} upgrade ${DEV_RELEASE_NAME} compute/ --reuse-values ${SET_FLAGS} --wait --timeout 300

git config user.email ${GIT_EMAIL}

git config user.name ${GIT_NAME}

git add configs/development.yaml

git commit -m "Updates imageTag to ${GIT_COMMIT:0:8}"

git push https://${GH_USR}:${GH_PSW}@github.com/esgf-compute/charts

export'''
        }

      }
    }

  }
  parameters {
    booleanParam(name: 'FORCE_PROVISIONER', defaultValue: false, description: 'Force provisioner build')
    booleanParam(name: 'FORCE_TASKS', defaultValue: false, description: 'Force tasks(celery) build')
    booleanParam(name: 'FORCE_WPS', defaultValue: false, description: 'Force WPS build')
    booleanParam(name: 'FORCE_THREDDS', defaultValue: false, description: 'Force THREDDS build')
  }
}