pipeline {
    agent none

    stages {
        stage('Test Django App') {
            agent { 
                docker { 
                    image 'conda-agent' 
                    args '--network outside'
                }
            }
            
            steps {
                git branch: 'devel', url: 'https://github.com/ESGF/esgf-compute-wps'
            
                sh '''#! /bin/bash
                    conda env create --name wps --file docker/common/environment.yml
                '''

        		sh '''#! /bin/bash
        		    pushd compute/wps/webapp/
        
        		    yarn install
        
           		    ./node_modules/.bin/webpack --config config/webpack.prod
        
        		    popd
        		'''
        
        		sh '''#! /bin/bash
        		    source activate wps
        
        		    pip install django-webpack-loader
        
        		    pip install -r compute/wps/tests/requirements.txt
        		'''
                
                sh '''#! /bin/bash
                    export WPS_TEST=1
                
                    export DJANGO_CONFIG_PATH="${PWD}/docker/common/django.properties"
		
		            mkdir -p /var/log/cwt
		
		            source activate wps
                    
                    pushd compute/
                    
                    python manage.py test --with-xunit --with-coverage --cover-xml --cover-package=wps || exit 1
                    
                    popd
                ''' 
            }
        }

        stage('Build docker images') {
            when { anyOf { branch 'bugfix-*'; branch 'release-*' } }
            
            agent any
            
            steps {
                script {
                    def version_index = env.BRANCH_NAME.indexOf('-')
                    def version = env.BRANCH_NAME.substring(version_index+1)
                    
                    sh "docker build -t jasonb87/cwt_common:${version} --build-arg TAG=${env.BRANCH_NAME} --network=outside docker/common"
                
                    sh "docker build -t jasonb87/cwt_celery:${version} --network=outside docker/celery"
                
                    sh "docker build -t jasonb87/cwt_wps:${version} --network=outside docker/wps"
                }
            }
        }
    }
    
    post {
        always {
            node('master') {
                step([$class: 'XUnitBuilder',
                    tools: [[$class: 'JUnitType', pattern: 'compute/nosetests.xml']]])
                
                step([$class: 'CoberturaPublisher', 
                    coberturaReportFile: 'compute/coverage.xml'])
            }
        }
    }
}
