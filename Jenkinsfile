pipeline {
  agent none
  environment {
    REGISTRY = "${env.BRANCH_NAME == "master" ? env.REGISTRY_PUBLIC : env.REGISTRY_PRIVATE}"
    IMAGE_PUSH = "${env.BRANCH_NAME.matches('devel|v.*') ? true : false}"
    CACHE_TYPE = "local"
    CACHE_PATH = "/nfs/buildkit-cache"
  }
  stages {
    stage("Components") {
      parallel {
        stage("Provisioner") {
          agent {
            label "jenkins-buildkit"
          }
          when {
            environment name: "IMAGE_PUSH", value: "true"
          }
          steps {
            container(name: "buildkit", shell: "/bin/sh") {
              sh "make provisioner OUTPUT_TYPE=registry IMAGE_PUSH=true TARGET=production"

              dir("charts") {
                git branch: "devel", url: "https://github.com/esgf-compute/charts.git"
              }

              lock("development") {
                sh """
helm repo add stable https://kubernetes-charts.storage.googleapis.com --ca-file /ssl/cspca.crt
helm dependency build charts/compute/
helm -n development upgrade $DEV_RELEASE_NAME charts/compute/ --set provisioner.imageTag=`make tag-provisioner` --wait --reuse-values --atomic
                """
              }
            }
          }
        }
        stage("Tasks") {
          agent {
            label "jenkins-buildkit"
          }
          stages {
            stage("Unittest") {
              steps {
                container(name: "buildkit", shell: "/bin/sh") {
                  sh """
make tasks IMAGE_PUSH=false TARGET=testresult

sed -i"" 's/timestamp="[^"]*"//' output/unittest.xml 

mv -f output/ tasks_output/

chmod -R 755 tasks_output

chown -R 1000:1000 tasks_output
                  """
                }

                archiveArtifacts artifacts: "tasks_output/*.xml"

                cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: "tasks_output/coverage.xml", conditionalCoverageTargets: "70, 0, 0", failUnhealthy: false, failUnstable: false, lineCoverageTargets: "80, 0, 0", maxNumberOfBuilds: 0, methodCoverageTargets: "80, 0, 0", onlyStable: false, sourceEncoding: "ASCII", zoomCoverageChart: false 

                junit "tasks_output/unittest.xml"
              }
            }
            stage("Push") {
              when {
                environment name: "IMAGE_PUSH", value: "true"
              }
              steps {
                container(name: "buildkit", shell: "/bin/sh") {
                  sh "make tasks OUTPUT_TYPE=registry IMAGE_PUSH=true TARGET=production"
                }
              }
            }
            stage("Deploy") {
              when {
                environment name: "IMAGE_PUSH", value: "true"
              }
              steps {
                container(name: "buildkit", shell: "/bin/sh") {
                  dir("charts") {
                    git branch: "devel", url: "https://github.com/esgf-compute/charts.git"
                  }

                  lock("development") {
                    sh """
helm repo add stable https://kubernetes-charts.storage.googleapis.com --ca-file /ssl/cspca.crt
helm dependency build charts/compute/
helm -n development upgrade $DEV_RELEASE_NAME charts/compute/ --set celery.imageTag=`make tag-tasks` --wait --reuse-values --atomic
                    """
                  }
                }
              }
            }
          }
        }
        stage("WPS") {
          agent {
            label "jenkins-buildkit"
          }
          stages {
            stage("Unittest") {
              steps {
                container(name: "buildkit", shell: "/bin/sh") {
                  sh """
make wps IMAGE_PUSH=false TARGET=testresult

sed -i"" 's/timestamp="[^"]*"//' output/unittest.xml 

mv -f output/ wps_output/

chmod -R 755 wps_output

chown -R 1000:1000 wps_output
                  """
                }

                archiveArtifacts artifacts: "wps_output/*.xml"

                cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: "wps_output/coverage.xml", conditionalCoverageTargets: "70, 0, 0", failUnhealthy: false, failUnstable: false, lineCoverageTargets: "80, 0, 0", maxNumberOfBuilds: 0, methodCoverageTargets: "80, 0, 0", onlyStable: false, sourceEncoding: "ASCII", zoomCoverageChart: false 

                junit "wps_output/unittest.xml"
              }
            }
            stage("Push") {
              when {
                environment name: "IMAGE_PUSH", value: "true"
              }
              steps {
                container(name: "buildkit", shell: "/bin/sh") {
                  sh "make wps OUTPUT_TYPE=registry IMAGE_PUSH=true TARGET=production"
                }
              }
            }
            stage("Deploy") {
              when {
                environment name: "IMAGE_PUSH", value: "true"
              }
              steps {
                container(name: "buildkit", shell: "/bin/sh") {
                  dir("charts") {
                    git branch: "devel", url: "https://github.com/esgf-compute/charts.git"
                  }

                  lock("development") {
                    sh """
helm repo add stable https://kubernetes-charts.storage.googleapis.com --ca-file /ssl/cspca.crt
helm dependency build charts/compute/
helm -n development upgrade $DEV_RELEASE_NAME charts/compute/ --set wps.imageTag=`make tag-wps` --wait --reuse-values --atomic
                    """
                  }
                }
              }
            }
          }
        }
        stage("THREDDS") {
          agent {
            label "jenkins-buildkit"
          }
          when {
            environment name: "IMAGE_PUSH", value: "true"
          }
          steps {
            container(name: "buildkit", shell: "/bin/sh") {
              sh "make thredds OUTPUT_TYPE=registry IMAGE_PUSH=true TARGET=production"

              dir("charts") {
                git branch: "devel", url: "https://github.com/esgf-compute/charts.git"
              }

              lock("development") {
                sh """
helm repo add stable https://kubernetes-charts.storage.googleapis.com --ca-file /ssl/cspca.crt
helm dependency build charts/compute/
helm -n development upgrade $DEV_RELEASE_NAME charts/compute/ --set thredds.imageTag=`make tag-wps` --wait --reuse-values --atomic
                """
              }
            }
          }
        } 
      }
    }
  }
}
