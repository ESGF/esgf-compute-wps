FROM continuumio/miniconda:4.2.12

ARG tag=master

RUN curl -sL https://deb.nodesource.com/setup_8.x | bash -a && \
	apt-get install -y nodejs && \
	curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
	echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
	apt-get update && \
	apt-get install -y --no-install-recommends git libpq-dev gcc yarn && \
	conda install -c conda-forge -c uvcdat esgf-compute-api cdms2 cdutil \
		genutil pyzmq gunicorn lxml

WORKDIR /var/www

RUN git clone https://github.com/ESGF/esgf-compute-wps compute && \
	cd compute && \
	git checkout ${tag} && \
	pip install -r requirements.txt  && \
	cd compute/wps && \
	yarn install && \
	./node_modules/.bin/webpack --config webpack.prod

WORKDIR /var/www/compute/compute

COPY entrypoint.sh entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]

CMD ["gunicorn", "-b", "0.0.0.0:8000", "--reload", "compute.wsgi"]
