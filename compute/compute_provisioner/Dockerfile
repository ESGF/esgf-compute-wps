FROM continuumio/miniconda3:4.7.10 as builder

ARG GIT_SHORT_COMMIT

ENV GIT_SHORT_COMMIT ${GIT_SHORT_COMMIT}
ENV VERSION 0.0.0

WORKDIR /src

COPY compute/compute_provisioner/ compute_provisioner/

RUN conda install conda-build && \
      conda build -c conda-forge compute_provisioner/

FROM continuumio/miniconda3:4.7.10 as production

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge --use-local compute-provisioner curl

ENV TINI_VERSION v0.18.0

RUN curl -fSLk https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/local/bin/tini && \
      chmod +x /usr/local/bin/tini

EXPOSE 7777 7778

ENV FRONTEND_PORT 7777
ENV BACKEND_PORT 7778

ENTRYPOINT ["/usr/local/bin/tini", "--"]

CMD ["python", "-m", "compute_provisioner.provisioner"]
