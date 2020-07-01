pipeline {
  agent none
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
              lock(resource: null, label: 'buildkit-cache', quantity: 1) {
                sh 'make provisioner CACHE_PATH=/nfs/buildkit-cache'
              }

              stash(name: 'update_provisioner.yaml', includes: 'update_provisioner.yaml')

              lock(resource: null, label: 'buildkit-cache') {
                sh 'make prune-cache'
              }
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
              sh '''ls -la /nfs/tasks-test-data/test_data

tar cf - /nfs/tasks-test-data/test_data/*.nc | (mkdir ${PWD}/compute/compute_tasks/test_data; tar xvf - --strip-components=3 -C ${PWD}/compute/compute_tasks/test_data)

ls -la ${PWD}/compute/compute_tasks/test_data'''
              lock(resource: null, label: 'buildkit-cache', quantity: 1) {
                sh 'make tasks TARGET=testresult CACHE_PATH=/nfs/buildkit-cache'
                sh 'make tasks TARGET=testdata CACHE_PATH=/nfs/buildkit-cache OUTPUT_PATH=/nfs/tasks-test-data/test_data'
                sh 'make tasks CACHE_PATH=/nfs/buildkit-cache'
              }

              stash(name: 'update_tasks.yaml', includes: 'update_tasks.yaml')

              lock(resource: null, label: 'buildkit-cache') {
                sh 'make prune-cache'
              }

              sh 'rm -rf ${PWD}/compute/compute_tasks/test_data'
              sh 'chown -R 10000:10000 output; touch output/*'
            }

            junit(testResults: 'output/unittest.xml', healthScaleFactor: 1)
            cobertura(coberturaReportFile: 'output/coverage.xml', autoUpdateHealth: true, autoUpdateStability: true)
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
              lock(resource: null, label: 'buildkit-cache', quantity: 1) {
                sh 'make wps TARGET=testresult CACHE_PATH=/nfs/buildkit-cache'
                sh 'make wps CACHE_PATH=/nfs/buildkit-cache'
              }

              stash(name: 'update_wps.yaml', includes: 'update_wps.yaml')

              lock(resource: null, label: 'buildkit-cache') {
                sh 'make prune-cache'
              }

              sh 'chown -R 10000:10000 output; touch output/*'
            }

            junit(testResults: 'output/unittest.xml', healthScaleFactor: 1)
            cobertura(autoUpdateHealth: true, autoUpdateStability: true, coberturaReportFile: 'output/coverage.xml')
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
              lock(resource: null, label: 'buildkit-cache', quantity: 1) {
                sh 'make thredds CACHE_PATH=/nfs/buildkit-cache'
              }

              stash(name: 'update_thredds.yaml', includes: 'update_thredds.yaml')

              lock(resource: null, label: 'buildkit-cache') {
                sh 'make prune-cache'
              }
            }

          }
        }

      }
    }

    stage('Deploy Dev') {
      agent {
        node {
          label 'jenkins-helm'
        }

      }
      when {
        branch 'devel'
      }
      environment {
        GH = credentials('ae3dd8dc-817a-409b-90b9-6459fb524afc')
        RELEASE = "${env.WPS_RELEASE_DEV}"
      }
      steps {
        container(name: 'helm', shell: '/bin/bash') {
          ws(dir: 'work') {
            script {
              try {
                unstash 'update_provisioner.yaml'
              } catch (err) { }

              try {
                unstash 'update_tasks.yaml'
              } catch (err) { }

              try {
                unstash 'update_wps.yaml'
              } catch (err) { }

              try {
                unstash 'update_thredds.yaml'
              } catch (err) { }
            }

            sh 'cat update_*.yaml > development.yaml || exit 0'
            archiveArtifacts(artifacts: 'development.yaml', fingerprint: true, allowEmptyArchive: true)
            sh '''#! /bin/bash
if [[ -e "development.yaml" ]]
then
  git clone https://github.com/esgf-compute/charts

  cd charts/

  make upgrade FILES="--values ../development.yaml" CA_FILE=/ssl/llnl.ca.pem TIMEOUT=8m
fi'''
            lock(resource: 'esgf-compute_charts') {
              sh '''#! /bin/bash
if [[ -e "development.yaml" ]]
then
  cd charts/

  python scripts/merge.py ../development.yaml development.yaml

  git status

  git config user.email ${GIT_EMAIL}
  git config user.name ${GIT_NAME}

  git add development.yaml

  git status

  git commit -m "Updates image tag."
  git push https://${GH_USR}:${GH_PSW}@github.com/esgf-compute/charts
fi'''
            }

          }

        }

      }
    }

    stage('Deploy Prod') {
      agent {
        node {
          label 'jenkins-helm'
        }

      }
      when {
        branch 'master'
      }
      environment {
        GH = credentials('ae3dd8dc-817a-409b-90b9-6459fb524afc')
        RELEASE = "${env.WPS_RELEASE_PROD}"
        CA_FILE = '/ssl/llnl.ca.pem'
      }
      steps {
        container(name: 'helm', shell: '/bin/bash') {
          ws(dir: 'work') {
            script {
              try {
                unstash 'update_provisioner.yaml'
              } catch (err) { }

              try {
                unstash 'update_tasks.yaml'
              } catch (err) { }

              try {
                unstash 'update_wps.yaml'
              } catch (err) { }

              try {
                unstash 'update_thredds.yaml'
              } catch (err) { }
            }

            sh 'cat update_*.yaml > production.yaml || exit 0'
            archiveArtifacts(artifacts: 'production.yaml', fingerprint: true, allowEmptyArchive: true)
            sh '''#! /bin/bash
if [[ -e "production.yaml" ]]
then
  git clone https://github.com/esgf-compute/charts

  cd charts/

  make upgrade FILES="--values ../production.yaml" TIMEOUT=8m
fi'''
            lock(resource: 'esgf-compute_charts') {
              sh '''#! /bin/bash
if [[ -e "production.yaml" ]]
then
  cd charts/

  python scripts/merge.py ../production.yaml compute/values.yaml

  git status

  git config user.email ${GIT_EMAIL}
  git config user.name ${GIT_NAME}

  git add compute/values.yaml

  git status

  git commit -m "Updates default image tags."

  git push https://${GH_USR}:${GH_PSW}@github.com/esgf-compute/charts
fi'''
            }

          }

        }

      }
    }

  }
  environment {
    REGISTRY = "${env.BRANCH_NAME == "master" ? env.REGISTRY_PUBLIC : env.REGISTRY_PRIVATE}"
  }
  parameters {
    booleanParam(name: 'FORCE_PROVISIONER', defaultValue: false, description: 'Force provisioner build')
    booleanParam(name: 'FORCE_TASKS', defaultValue: false, description: 'Force tasks(celery) build')
    booleanParam(name: 'FORCE_WPS', defaultValue: false, description: 'Force WPS build')
    booleanParam(name: 'FORCE_THREDDS', defaultValue: false, description: 'Force THREDDS build')
  }
}
