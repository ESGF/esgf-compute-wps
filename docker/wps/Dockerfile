FROM jasonb87/cwt_common:latest

RUN curl -sL https://deb.nodesource.com/setup_8.x | bash -a && \
	apt-get install -y nodejs && \
	curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
	echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
	apt-get update && \
	apt-get install -y --no-install-recommends yarn && \
	apt-get autoclean -y && \
	apt-get autoremove -y

WORKDIR /var/www/compute/compute/wps/webapp

RUN yarn install && \
	./node_modules/.bin/webpack --config config/webpack.prod

RUN conda install -n wps -c conda-forge gunicorn=19.3.0 && \
	conda clean --all -y

RUN PATH=/opt/conda/envs/wps/bin:$PATH pip install django-webpack-loader

WORKDIR /

COPY entrypoint.sh entrypoint.sh

COPY healthcheck.sh healthcheck.sh

EXPOSE 8000

HEALTHCHECK CMD ./healthcheck.sh

ENTRYPOINT ["./entrypoint.sh"]
