node('build-pod') {
  stage('Checkout') {
    checkout scm
  }

  stage('Test Django app') {
    container('conda') {
      sh "conda env create -p ${HOME}/wps -f docker/common/environment.yml"

      sh ''' #!/bin/bash
      . /opt/conda/etc/profile.d/conda.sh

      conda activate ${HOME}/wps

      conda install -y -c conda-forge flake8

      pip install -r compute/wps/tests/requirements.txt

      mkdir -p /var/log/cwt
      '''

      sh ''' #!/bin/bash
      . /opt/conda/etc/profile.d/conda.sh

      conda activate ${HOME}/wps

      WPS_TEST=1 DJANGO_CONFIG_PATH=${WORKSPACE}/docker/common/django.properties \
      python ${WORKSPACE}/compute/manage.py test ${WORKSPACE}/compute/wps/tests \
      --with-xunit --xunit-file nosetests.xml \
      --with-coverage --cover-tests --cover-package wps --cover-xml --cover-xml-file cover.xml

      flake8 --format=pylint --output-file=flake8.xml --exit-zero

      sed -i 's/ skip="[^"]*"//g' nosetests.xml
      '''

      archiveArtifacts 'nosetests.xml'

      archiveArtifacts 'cover.xml'

      archiveArtifacts 'flake8.xml'

      xunit([JUnit(deleteOutputFiles: true, failIfNotNew: true, pattern: 'nosetests.xml', skipNoTestFiles: true, stopProcessingIfError: true)])

      cobertura(coberturaReportFile: 'cover.xml')

      def flake8 = scanForIssues filters: [
        excludeFile('.*manage.py'), 
        excludeFile('.*compute/scripts.*'),
        excludeFile('.*compute/wps/migrations.*')
      ], tool: flake8(pattern: 'flake8.xml')

      publishIssues issues: [flake8], filters: [includePackage('wps')]
    }   
  }

  stage('Build docker images') {
    def parts = env.BRANCH_NAME.split('/')

    env.TAG = parts[parts.length-1]

    container(name: 'kaniko', shell: '/busybox/sh') {
      sh '''#!/busybox/sh
        /kaniko/executor --cache --cache-dir=/cache --context=`pwd` \
        --destination=${LOCAL_REGISTRY}/common:${TAG} \
        --dockerfile `pwd`/docker/common/Dockerfile --insecure-registry \
        ${LOCAL_REGISTRY}
      '''
    }

    container('dind') {
      sh ''' #!/bin/bash
      docker pull ${LOCAL_REGISTRY}/common:${TAG}

      docker tag ${LOCAL_REGISTRY}/common:${TAG} jasonb87/cwt_common:${TAG}

      docker push jasonb87/cwt_common:${TAG}
      '''
    }

    container(name: 'kaniko', shell: '/busybox/sh') {
      sh '''#!/busybox/sh
        /kaniko/executor --cache --cache-dir=/cache --context=`pwd` \
        --destination=${LOCAL_REGISTRY}/wps:${TAG} \
        --dockerfile `pwd`/docker/wps/Dockerfile --insecure-registry \
        ${LOCAL_REGISTRY}
      '''

        sh '''#!/busybox/sh
        /kaniko/executor --cache --cache-dir=/cache --context=`pwd` \
        --destination=${LOCAL_REGISTRY}/celery:${TAG} \
        --dockerfile `pwd`/docker/celery/Dockerfile --insecure-registry \
        ${LOCAL_REGISTRY}
      '''
    }

    container('dind') {
      sh ''' #!/bin/bash
      docker pull ${LOCAL_REGISTRY}/wps:${TAG}

      docker tag ${LOCAL_REGISTRY}/wps:${TAG} jasonb87/cwt_wps:${TAG}

      docker push jasonb87/cwt_wps:${TAG}
      '''

      sh ''' #!/bin/bash
      docker pull ${LOCAL_REGISTRY}/celery:${TAG}

      docker tag ${LOCAL_REGISTRY}/celery:${TAG} jasonb87/cwt_celery:${TAG}

      docker push jasonb87/cwt_celery:${TAG}
      '''
    }
  }
}
