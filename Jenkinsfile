pipeline {
  agent none
  stages {
    stage('Checkout Chart') {
      agent {
        node {
          label 'jenkins-buildkit'
        }

      }
      steps {
        container(name: 'helm', shell: '/bin/bash') {
          sh 'git clone https://github.com/esgf-compute/charts'
        }

      }
    }

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

    stage('Push Container') {
      parallel {
        stage('provisioner') {
          agent {
            node {
              label 'jenkins-buildkit'
            }

          }
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
              sh 'make provisioner REGISTRY=${OUTPUT_REGISTRY}'
            }

          }
        }

        stage('tasks') {
          agent {
            node {
              label 'jenkins-buildkit'
            }

          }
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
              sh 'make tasks REGISTRY=${OUTPUT_REGISTRY}'
            }

            container(name: 'helm', shell: '/bin/bash') {
              sh 'la -la'
            }

          }
        }

        stage('wps') {
          agent {
            node {
              label 'jenkins-buildkit'
            }

          }
          steps {
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
          steps {
            container(name: 'buildkit', shell: '/bin/sh') {
              sh 'make thredds REGISTRY=${OUTPUT_REGISTRY}'
            }

          }
        }

      }
    }

    stage('Test') {
      agent {
        node {
          label 'jenkins-helm'
        }

      }
      steps {
        container(name: 'helm', shell: '/bin/bash') {
          sh '''ls -la output/

'''
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