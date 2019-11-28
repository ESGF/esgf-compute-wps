pipeline {
  agent {
    node {
      label 'jenkins-buildkit'
    }

  }
  stages {
    stage('Build Provisioner') {
      steps {
        sh 'make'
      }
    }

  }
}