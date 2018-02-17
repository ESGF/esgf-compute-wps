FROM esgfhub/esgf-cog:1.4

COPY wait_for_postgres.sh /usr/local/bin/wait_for_postgres.sh

COPY cog_settings.cfg /usr/local/cog/cog_config/cog_settings.cfg

COPY entrypoint.sh entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

CMD ["localhost", "false", "true", "http://localhost/wps/home/configure", "true"]
