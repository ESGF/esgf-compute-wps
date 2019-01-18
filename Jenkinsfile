pipeline {
  agent any;
  
  stages {
    stage('Build frontend') {
      steps {
        checkout scm

        dir('compute/wps/webapp') {
          sh 'yarn install'

          sh './node_modules/.bin/webpack --config config/webpack.prod.js'
        }
      }
    }

    stage('Install dependencies') {
      steps {
        sh '''#! /bin/bash
          export
        
          conda env create -n wps-${NODE_NAME} --file docker/common/environment.yml

          source activate wps-${NODE_NAME}

          conda install -c conda-forge gunicorn=19.3.0
          
          pip install django-webpack-loader bjoern

          pip install -r compute/wps/tests/requirements.txt
        '''
          
        sh '''#! /bin/bash
          export WPS_TEST=1
          export DJANGO_CONFIG_PATH=${PWD}/docker/common/django.properties

          source activate wps-${NODE_NAME}

          cd compute

          python manage.py test --with-xunit --xunit-file ../xunit.xml --with-coverage --cover-xml --cover-xml-file ../coverage.xml --cover-package=wps

          sed 's/skip=/skipped=/' -i.bak ../xunit.xml
        '''
      }
    }
  }

  post {
    always {
      sh 'conda env remove -y -n wps-${NODE_NAME}'
        
      archiveArtifacts 'xunit.xml'

      archiveArtifacts 'coverage.xml'
    }

    success {
      xunit testTimeMargin: '3000', thresholdMode: 1, thresholds: [], tools: [JUnit(deleteOutputFiles: true, failIfNotNew: true, pattern: 'xunit.xml', skipNoTestFiles: true, stopProcessingIfError: true)]

      cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'coverage.xml', conditionalCoverageTargets: '70, 0, 0', failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false
    }    
  }
}
