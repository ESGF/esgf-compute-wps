pipeline {
    agent any

    stages {
        stage('Test Django App') {
            steps {
                sh 'cp docker/wps/django.properties django.properties'
                
                withEnv(['PATH+EXTRA=/opt/conda/bin', 'WPS_TEST=1', 'DJANGO_CONFIG_PATH=./django.properties']) {
                    sh 'sudo -i conda env create --name wps --file docker/common/environment.yml || exit 0'
                    
                    sh '''#!/bin/bash
                        . /opt/conda/bin/activate wps
                        
                        sudo /opt/conda/bin/pip install django-webpack-loader
                        
                        sudo /opt/conda/bin/pip install -r compute/wps/tests/requirements.txt
                        
                        python compute/manage.py test --with-xunit --with-coverage --cover-xml --cover-xml-file coverage.xml --cover-package wps compute/wps/tests || exit 0
                    '''
                }
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
        
        stage('Choose push docker containers') {
            steps {
                script {
                    try {
                        timeout(time: 1, unit: 'HOURS') {
                            env.DOCKER_PUSH = input(message: 'User input required', parameters:[
                                choice(name: 'Push to docker registry', choices: 'no\nyes')])
                        }
                    } catch(err) {
                        env.DOCKER_PUSH = 'no'
                    }
                }
            }
        }
        
        stage('Push docker containers') {
            when {
                environment name: 'DOCKER_PUSH', value: 'yes'
            }
            
            steps {
                sh 'docker push jasonb87/cwt_common:2.0.0'
                
                sh 'docker push jasonb87/cwt_wps:2.0.0'
                
                sh 'docker push jasonb87/cwt_celery:2.0.0'
                
                sh 'docker push jasonb87/cwt_thredds:4.6.10'

            }
        }
    }
    
    post {
        always{
            step([$class: 'XUnitBuilder',
                thresholds: [[$class: 'FailedThreshold', failureThreshold: '20']],
                tools: [[$class: 'JUnitType', pattern: 'nosetests.xml']]])
                
            step([$class: 'CoberturaPublisher', 
                autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: '**/coverage.xml', 
                failUnhealthy: false, failUnstable: false, maxNumberOfBuilds: 0, onlyStable: false, 
                sourceEncoding: 'ASCII', zoomCoverageChart: false])

        }
    }
}
