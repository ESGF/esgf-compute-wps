FROM continuumio/miniconda3:4.7.10 as builder

ARG GIT_SHORT_COMMIT

ENV GIT_SHORT_COMMIT ${GIT_SHORT_COMMIT}

RUN conda install -y conda-build

WORKDIR /src

COPY compute/compute_wps/ compute_wps/

RUN echo "__version__ = '${GIT_SHORT_COMMIT}'" > compute_wps/compute_wps/_version.py 

RUN conda build -c conda-forge -c cdat --use-local compute_wps/

FROM builder as dev

COPY docker/wps/django.properties /etc/config/django.properties

RUN apt-get update && \
      apt-get install -y --no-install-recommends vim curl && \
      rm -rf /var/lib/apt/lists/* && \
      conda install -c conda-forge -c cdat --use-local --only-deps compute-wps flake8 pytest python=3.6 && \
      pip install -e compute_wps/

ENV TINI_VERSION v0.18.0

RUN curl -fSLk https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/local/bin/tini && \
      chmod +x /usr/local/bin/tini

WORKDIR /

RUN django-admin startproject compute

WORKDIR /compute/compute

COPY docker/wps/settings.py .
COPY docker/wps/urls.py .
COPY docker/wps/wsgi.py .

WORKDIR /src/compute/compute_wps

COPY docker/wps/development.sh .

ENTRYPOINT ["/usr/local/bin/tini", "--"]

CMD ["/bin/bash"]

FROM continuumio/miniconda3:4.7.10

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

WORKDIR /

RUN conda install -c conda-forge -c cdat --use-local compute-wps curl python=3.6 && \
      django-admin startproject compute 

ENV TINI_VERSION v0.18.0

RUN curl -fSLk https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/local/bin/tini && \
      chmod +x /usr/local/bin/tini

COPY docker/wps/django.properties /etc/config/

WORKDIR /compute

COPY docker/wps/settings.py compute/
COPY docker/wps/urls.py compute/
COPY docker/wps/wsgi.py compute/
COPY docker/wps/app.py .
COPY docker/wps/entrypoint.sh .

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/tini", "--"]

CMD ["./entrypoint.sh"]
