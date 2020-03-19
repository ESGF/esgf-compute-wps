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
              sh '''make provisioner REGISTRY=${OUTPUT_REGISTRY}

export PROVISIONER=$(git rev-parse --short HEAD)'''
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

export TASKS=$(git rev-parse --short HEAD)'''
              sh 'chown -R 10000:10000 /output'
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

export WPS=$(git rev-parse --short HEAD)'''
              sh 'chown -R 10000:10000 /output'
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

export THREDDS=$(git rev-parse --short HEAD)'''
            }

          }
        }

      }
    }

    stage('error') {
      agent {
        node {
          label 'jenkins-buildkit'
        }

      }
      steps {
        container(name: 'buildkit', shell: '/bin/sh') {
          sh '''echo "PROVISIONER ${PROVISIONER}"
echo "TASKS ${TASKS}"
echo "WPS ${WPS}"
echo "THREDDS ${THREDDS}"'''
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