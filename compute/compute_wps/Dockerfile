FROM continuumio/miniconda3:4.7.10 as builder

ARG GIT_SHORT_COMMIT

ENV GIT_SHORT_COMMIT ${GIT_SHORT_COMMIT}

RUN conda install -y conda-build

WORKDIR /src

COPY compute/compute_wps/ compute_wps/

RUN conda build -c conda-forge -c cdat --use-local compute_wps/

FROM continuumio/miniconda3:4.7.10 as production

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

WORKDIR /

RUN conda install -c conda-forge -c cdat --use-local compute-wps curl python=3.6 && \
      django-admin startproject compute 

ENV TINI_VERSION v0.18.0

RUN curl -fSLk https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/local/bin/tini && \
      chmod +x /usr/local/bin/tini

COPY compute/compute_wps/django.properties /etc/config/

WORKDIR /compute

COPY compute/compute_wps/settings.py compute/
COPY compute/compute_wps/urls.py compute/
COPY compute/compute_wps/wsgi.py compute/
COPY compute/compute_wps/app.py .
COPY compute/compute_wps/entrypoint.sh .

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/tini", "--"]

CMD ["./entrypoint.sh"]

FROM production as testing

ENV SECRET_KEY "abcd"
ENV WPS_DEV 1
ENV WPS_TEST 1
ENV OAUTH2_CLIENT_ID ""
ENV OAUTH2_CLIENT_SECRET ""
ENV PROVISIONER_FRONTEND ""

COPY compute/compute_wps/django.properties /etc/confiog/django.properties
COPY compute/compute_wps/pytest.ini .

COPY --from=builder /src/compute_wps .

RUN conda install -c conda-forge mock pytest-django pytest-cov

RUN pytest -vvv --junit-xml=unittest.xml --cov=compute_wps --cov-report=html:coverage/ --cov-report=xml || exit 0

FROM scratch as testresult

COPY --from=testing /compute/unittest.xml .
COPY --from=testing /compute/coverage coverage
COPY --from=testing /compute/coverage.xml .
