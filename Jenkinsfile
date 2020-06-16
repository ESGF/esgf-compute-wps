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
              sh '''TAG="$(cat compute/compute_provisioner/VERSION)"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
REGISTRY="${REGISTRY_PRIVATE}"

if [[ "${BRANCH}" == "master" ]] 
then 
  REGISTRY="${REGISTRY_PUBLIC}"
elif [[ "${BRANCH}" == "devel" ]]
then
  TAG="${TAG}_${BRANCH}_${BUILD_NUMBER}"
fi

# Build and push production image
make provisioner REGISTRY=${REGISTRY} \\
  CACHE_PATH=/nfs/buildkit-cache \\
  OUTPUT_PATH=${PWD}/output \\
  TAG=${TAG}

echo -e "provisioner:\\n  imageTag: ${TAG}\\n" > update_provisioner.yaml'''
              stash(name: 'update_provisioner.yaml', includes: 'update_provisioner.yaml')
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
              sh '''TAG="$(cat compute/compute_tasks/VERSION)"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
REGISTRY="${REGISTRY_PRIVATE}"

if [[ "${BRANCH}" == "master" ]] 
then 
  REGISTRY="${REGISTRY_PUBLIC}"
elif [[ "${BRANCH}" == "devel" ]]
then
  TAG="${TAG}_${BRANCH}_${BUILD_NUMBER}"
fi

TEST_DATA_SRC=/nfs/tasks-test-data/test_data
TEST_DATA_DST=${PWD}/compute/compute_tasks/test_data
mkdir -p ${TEST_DATA_DST}
find ${TEST_DATA_SRC} -type f -exec sh -c \'cp ${1} ${TEST_DATA_DST}/${1##*/}\' sh {} \\;

rm -rf ${PWD}/compute/compute_tasks/test_data

# Run the unit tests and copy results
make tasks TARGET=testresult \\
  REGISTRY=${REGISTRY} \\
  CACHE_PATH=/nfs/buildkit-cache \\
  OUTPUT_PATH=${PWD}/output \\
  TAG=${TAG}

find ${TEST_DATA_DST} -type f -exec unlink {} \\;

# Update test data cache
make tasks TARGET=testdata \\
  REGISTRY=${REGISTRY} \\
  CACHE_PATH=/nfs/buildkit-cache \\
  OUTPUT_PATH=/nfs/tasks-test-data \\
  TAG=${TAG}

# Push production image
make tasks REGISTRY=${REGISTRY} \\
  CACHE_PATH=/nfs/buildkit-cache \\
  TAG=${TAG}

echo -e "celery:\\n  imageTag: ${TAG}\\n" > update_tasks.yaml'''
              sh '''chown -R 10000:10000 output

touch output/*'''
              stash(name: 'update_tasks.yaml', includes: 'update_tasks.yaml')
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
              sh '''TAG="$(cat compute/compute_wps/VERSION)"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
REGISTRY="${REGISTRY_PRIVATE}"

if [[ "${BRANCH}" == "master" ]] 
then 
  REGISTRY="${REGISTRY_PUBLIC}"
elif [[ "${BRANCH}" == "devel" ]]
then
  TAG="${TAG}_${BRANCH}_${BUILD_NUMBER}"
fi

# Run unit tests and copy output
make wps TARGET=testresult \\
  REGISTRY=${REGISTRY} \\
  CACHE_PATH=/nfs/buildkit-cache \\
  OUTPUT_PATH=${PWD}/output \\
  TAG=${TAG}

make wps REGISTRY=${REGISTRY} \\
  CACHE_PATH=/nfs/buildkit-cache \\
  TAG=${TAG}

echo -e "wps:\\n  imageTag: ${TAG}\\n" > update_wps.yaml'''
              sh '''chown -R 10000:10000 output

touch output/*'''
              stash(name: 'update_wps.yaml', includes: 'update_wps.yaml')
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
              sh '''make thredds REGISTRY=${REGISTRY} CACHE_PATH=/nfs/buildkit-cache

TAG="$(cat docker/thredds/VERSION)"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
REGISTRY="${REGISTRY_PRIVATE}"

if [[ "${BRANCH}" == "master" ]] 
then 
  REGISTRY="${REGISTRY_PUBLIC}"
elif [[ "${BRANCH}" == "devel" ]]
then
  TAG="${TAG}_${BRANCH}_${BUILD_NUMBER}"
fi

make thredds REGISTRY=${REGISTRY} \\
  CACHE_PATH=/nfs/buildkit-cache \\
  TAG=${TAG}

echo -e "thredds:\\n  imageTag: ${TAG}\\n" > update_thredds.yaml'''
              stash(name: 'update_thredds.yaml', includes: 'update_thredds.yaml')
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

            sh '''#! /bin/bash

if [[ ! -z "$(find . -type f -iname \'update_*.yaml\')" ]]
then
  cat update_*.yaml > development.yaml

  cat development.yaml

  git clone https://github.com/esgf-compute/charts

  helm3 status ${DEV_RELEASE_NAME}

  helm3 upgrade ${DEV_RELEASE_NAME} charts/compute --values development.yaml --reuse-values --wait --timeout 2m | exit 1
fi'''
            sh '''#! /bin/bash

if [[ -e "development.yaml" ]]
then
  python charts/compute/scripts/merge.py development.yaml charts/development.yaml

  cd charts/

  git status

  git config user.email ${GIT_EMAIL}
  git config user.name ${GIT_NAME}

  git add development.yaml

  git status

  git commit -m "Updates image tag."
  git push https://${GH_USR}:${GH_PSW}@github.com/esgf-compute/charts
fi'''
            archiveArtifacts(artifacts: 'development.yaml', fingerprint: true, allowEmptyArchive: true)
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
      }
      steps {
        container(name: 'helm', shell: '/bin/bash') {
          ws(dir: 'work') {
            sh '''#! /bin/bash

GIT_DIFF="$(git diff --name-only ${GIT_COMMIT} ${GIT_PREVIOUS_COMMIT})"
HELM_ARGS="--atomic --timeout 2m --reuse-values"
UPDATE_SCRIPT="charts/scripts/update_config.py"
VALUES="charts/compute/values.yaml"

git clone https://github.com/esgf-compute/charts

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_provisioner/)" ]] || [[ "${FORCE_PROVISIONER}" == "true" ]]
then
  TAG="$(cat compute/compute_provisioner/VERSION)"

  python ${UPDATE_SCRIPT} ${VALUES} provisioner ${TAG}

  helm3 upgrade ${PROD_RELEASE_NAME} charts/compute/ --set provisioner.imageTag=${TAG} ${HELM_ARGS}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_wps/)" ]] || [[ "${FORCE_WPS}" == "true" ]]
then
  TAG="$(cat compute/compute_wps/VERSION)"

  python ${UPDATE_SCRIPT} ${VALUES} wps ${TAG}

  helm3 upgrade ${PROD_RELEASE_NAME} charts/compute/ --set wps.imageTag=${TAG} ${HELM_ARGS}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /compute_tasks/)" ]] || [[ "${FORCE_TASKS}" == "true" ]]
then
  TAG="$(cat compute/compute_tasks/VERSION)"

  python ${UPDATE_SCRIPT} ${VALUES} celery ${TAG}

  helm3 upgrade ${PROD_RELEASE_NAME} charts/compute/ --set celery.imageTag=${TAG} ${HELM_ARGS}
fi

if [[ ! -z "$(echo ${GIT_DIFF} | grep /docker/thredds/)" ]] || [[ "${FORCE_THREDDS}" == "true" ]]
then
  TAG="$(cat docker/thredds/VERSION)"

  python ${UPDATE_SCRIPT} ${VALUES} thredds ${TAG}

  helm3 upgrade ${PROD_RELEASE_NAME} charts/compute/ --set thredds.imageTag=${TAG} ${HELM_ARGS}
fi

helm3 status ${PROD_RELEASE_NAME}

cd charts/

git status

git config user.email ${GIT_EMAIL}
git config user.name ${GIT_NAME}
git add charts/compute/values.yaml
git status
git commit -m "Updates image tag."
git push https://${GH_USR}:${GH_PSW}@github.com/esgf-compute/charts'''
          }

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