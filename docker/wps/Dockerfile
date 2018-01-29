FROM jasonb87/cwt_common:latest

RUN curl -sL https://deb.nodesource.com/setup_8.x | bash -a && \
	curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
	echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
	apt-get update && \
	apt-get install -y --no-install-recommends yarn build-essential libev-dev nodejs && \
	conda install -n wps -c conda-forge gunicorn=19.3.0 && \
	PATH=/opt/conda/envs/wps/bin:$PATH pip install django-webpack-loader bjoern && \	
	apt-get autoremove -y build-essential && \
	rm -rf /var/lib/apt/lists/* && \
	conda clean --all -y

RUN cd /var/www/compute/compute/wps/webapp && \
	yarn install && \
	./node_modules/.bin/webpack --config config/webpack.prod

COPY entrypoint.sh entrypoint.sh

COPY healthcheck.sh healthcheck.sh

COPY app.py app.py

RUN wget https://raw.githubusercontent.com/eficode/wait-for/master/wait-for -O wait-for && \
	chmod +x wait-for

EXPOSE 8000

HEALTHCHECK CMD ./healthcheck.sh

ENTRYPOINT ["./entrypoint.sh"]
