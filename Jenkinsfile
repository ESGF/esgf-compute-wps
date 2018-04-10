pipeline {
    agent any
    
    stages {
        stage('Test Django App') {
            steps {
                //git branch: 'bugfix-2.0.1', url: 'https://github.com/ESGF/esgf-compute-wps'
            
                sh 'conda env create --name wps --file docker/common/environment.yml'

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
                    
                    pushd compute/
                    
                    python manage.py test --with-xunit --with-coverage --cover-xml --cover-package=wps || exit 1
                    
                    popd
                ''' 
            }
        }
    }
    
    post {
        always {
            step([$class: 'XUnitBuilder',
                tools: [[$class: 'JUnitType', pattern: 'compute/nosetest.xml']]])
            
            step([$class: 'CoberturaPublisher', 
                coberturaReportFile: 'compute/coverage.xml'])
            
            sh 'conda env remove --name wps'
        }
    }
}
