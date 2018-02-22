pipeline {
    agent any

    stages {
        stage('Repository clone') {
            steps {
                git 'https://github.com/ESGF/esgf-compute-wps'
            }
        }
        
        stage('Build docker containers') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'hub-docker', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                    sh 'docker login -u $USERNAME -p $PASSWORD'
                }
                
                sh 'docker build -t jasonb87/cwt_common:2.0.0 docker/common'
                
                sh 'docker build -t jasonb87/cwt_wps:2.0.0 docker/wps'
                
                sh 'docker build -t jasonb87/cwt_celery:2.0.0 docker/celery'
                
                sh 'docker build -t jasonb87/cwt_thredds:4.6.10 docker/thredds'
            }
        }
        
        stage('Push docker containers') {
            steps {
                sh 'docker push jasonb87/cwt_common:2.0.0'
                
                sh 'docker push jasonb87/cwt_wps:2.0.0'
                
                sh 'docker push jasonb87/cwt_celery:2.0.0'
                
                sh 'docker push jasonb87/cwt_thredds:4.6.10'
            }
        }
    }
}
