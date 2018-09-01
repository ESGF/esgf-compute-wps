pipeline {
  agent any;

  stages {
    stage('Build frontend') {
      steps {
        checkout scm
        //checkout([$class: 'GitSCM', branches: [[name: '*/cdat_update']], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[url: 'https://github.com/ESGF/esgf-compute-wps']]])

        dir('compute/wps/webapp') {
          sh 'yarn install'

          sh './node_modules/.bin/webpack --config config/webpack.prod.js'
        }
      }
    }

    stage('Install dependencies') {
      steps {
        sh '''#! /bin/bash
        export WPS_TEST=1
        export DJANGO_CONFIG_PATH=${PWD}/docker/common/django.properties

        conda env remove -n wps -q -y > /dev/null 2>&1 || exit 1

        conda env create -n wps --file docker/common/environment.yml

        source activate wps

        conda install -c conda-forge gunicorn=19.3.0

        pip install django-webpack-loader bjoern

        pip install -r compute/wps/tests/requirements.txt

        cd compute

        python manage.py test
        '''
      }
    }
  }
}
