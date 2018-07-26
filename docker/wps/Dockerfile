FROM jasonb87/cwt_common:2.1.0

RUN curl -sL https://deb.nodesource.com/setup_8.x | bash -a && \
	curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
	echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
	apt-get update && \
	apt-get install -y --no-install-recommends yarn build-essential nodejs && \
	apt-get autoremove -y build-essential && \
	rm -rf /usr/share/doc/ && \
	rm -rf /usr/share/man/ && \
	rm -rf /usr/share/locale && \
	rm -rf /var/lib/apt/lists/*

RUN cd /var/www/compute/compute/wps/webapp && \
	yarn install && \
	./node_modules/.bin/webpack --config config/webpack.prod

WORKDIR /var/www/compute/docker/wps

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
