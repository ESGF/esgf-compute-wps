FROM quay.io/travisci/travis-python

ENV BRANCH="master"

SHELL ["/bin/bash", "--login", "-c"]

USER travis

RUN rvm install 2.3.0 && \
	rvm use --default 2.3.0

RUN cd ${HOME}/builds && \
	git clone https://github.com/travis-ci/travis-build.git && \
	cd travis-build && \
	gem install travis && \
	travis version && \
	ln -s `pwd` ~/.travis/travis-build && \
	bundle install && \
	bundler add travis && \
	bundler binstubs travis && \
	echo "y" | ${HOME}/.travis/travis-build/bin/travis version

COPY entrypoint.sh entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
