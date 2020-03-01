FROM continuumio/miniconda3:4.7.10 as builder

RUN conda update -n base conda && \
      conda install -y conda-build

WORKDIR /src

COPY compute/compute_tasks compute_tasks/

RUN conda build -c conda-forge -c cdat --use-local compute_tasks/

FROM continuumio/miniconda3:4.7.10 as production

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

RUN conda install --yes -c conda-forge -c cdat --use-local compute-tasks curl tini && \
      conda clean -afy

RUN mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

RUN apt-get update && \
      apt-get install -y --no-install-recommends dnsutils && \
      rm -rf /var/lib/apt/lists/*

WORKDIR /

COPY compute/compute_tasks/entrypoint.sh .
COPY compute/compute_tasks/healthcheck.sh .

EXPOSE 8000

ENTRYPOINT ["tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh", "-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]

ARG GIT_SHORT_COMMIT

ENV CONTAINER_VER ${GIT_SHORT_COMMIT}
ENV prometheus_multiproc_dir /metrics

FROM production as testing

WORKDIR /testing

COPY compute/compute_tasks/compute_tasks compute_tasks/
COPY compute/compute_tasks/setup.py .
COPY compute/compute_tasks/setup.cfg .
COPY compute/compute_tasks/conftest.py .

ARG MPC_HOST
ARG MPC_USERNAME
ARG MPC_PASSWORD

ENV MPC_HOST ${MPC_HOST}
ENV MPC_USERNAME ${MPC_USERNAME}
ENV MPC_PASSWORD ${MPC_PASSWORD}

RUN conda install --yes -c conda-forge pytest pytest-mock pytest-cov myproxyclient pydap && \
      mkdir /metrics && \
      conda remove compute-tasks --force -y && \
      pip install -e .

RUN pytest -vvv -m "not myproxyclient" --junit-xml=unittesting.xml --cov=compute_tasks --cov-report=xml || exit 0
# RUN pytest -vvv -m "myproxyclient" --junit-xml=unittesting-mpc.xml --cov=compute_tasks --cov-report=xml:coverage-mpc.xml || exit 0

FROM scratch as testresult

COPY --from=testing /testing/unittesting.xml .
# COPY --from=testing /testing/unittesting-mpc.xml .
COPY --from=testing /testing/coverage.xml .
# COPY --from=testing /testing/coverage-mpc.xml .
