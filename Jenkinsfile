pipeline {
  agent {
    node {
      label 'jenkins-helm'
    }

  }
  stages {
    stage('Build/Unittest') {
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
              sh '''make provisioner REGISTRY=${OUTPUT_REGISTRY}
'''
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
              sh '''make tasks REGISTRY=${OUTPUT_REGISTRY} TARGET=testresult
'''
              sh '''chown -R 10000:10000 output

touch output/*'''
            }

            junit(testResults: 'output/unittest.xml', healthScaleFactor: 1)
            cobertura(coberturaReportFile: 'output/coverage.xml', autoUpdateHealth: true, autoUpdateStability: true)
            container(name: 'buildkit', shell: '/bin/sh') {
              sh 'make tasks REGISTRY=${OUTPUT_REGISTRY}'
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
              sh '''make wps REGISTRY=${OUTPUT_REGISTRY} TARGET=testresult
'''
              sh '''chown -R 10000:10000 output

touch output/*'''
            }

            junit(testResults: 'output/unittest.xml', healthScaleFactor: 1)
            cobertura(autoUpdateHealth: true, autoUpdateStability: true, coberturaReportFile: 'output/coverage.xml')
            container(name: 'buildkit', shell: '/bin/sh') {
              sh 'make wps REGISTRY=${OUTPUT_REGISTRY}'
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
              sh '''make thredds REGISTRY=${OUTPUT_REGISTRY}
'''
            }

          }
        }

      }
    }

    stage('Update Helm Chart') {
      agent {
        node {
          label 'jenkins-buildkit'
        }

      }
      when {
        anyOf {
          branch 'master'
          branch 'devel'
        }

      }
      environment {
        GH = credentials('ae3dd8dc-817a-409b-90b9-6459fb524afc')
      }
      steps {
        container(name: 'helm', shell: '/bin/bash') {
          sh '''#! /bin/bash

GIT_DIFF="$(git diff --name-only ${GIT_COMMIT} ${GIT_PREVIOUS_COMMIT})"

git clone -b devel https://github.com/esgf-compute/charts

cd charts/

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_provisioner/)" ]] || [[ "${FORCE_PROVISIONER}" == "true" ]]
then
  python scripts/update_config.py compute/values.yaml provisioner ${GIT_COMMIT:0:8}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_wps/)" ]] || [[ "${FORCE_WPS}" == "true" ]]
then
  python scripts/update_config.py compute/values.yaml wps ${GIT_COMMIT:0:8}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_tasks/)" ]] || [[ "${FORCE_TASKS}" == "true" ]]
then
  python scripts/update_config.py compute/values.yaml celery ${GIT_COMMIT:0:8}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /docker/thredds/)" ]] || [[ "${FORCE_THREDDS}" == "true" ]]
then
  python scripts/update_config.py compute/values.yaml thredds ${GIT_COMMIT:0:8}
fi

git config user.email ${GIT_EMAIL}
git config user.name ${GIT_NAME}
git add compute/values.yaml
git status
git commit -m "Updates imageTag to ${GIT_COMMIT:0:8}"
git push https://${GH_USR}:${GH_PSW}@github.com/esgf-compute/charts'''
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