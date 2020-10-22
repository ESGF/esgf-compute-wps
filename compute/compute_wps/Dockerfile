ARG BASE_IMAGE
FROM $BASE_IMAGE as builder

WORKDIR /build

RUN conda update -n base conda && \
      conda install -c conda-forge -y conda-build conda-smithy && \
      conda smithy ci-skeleton compute_wps

WORKDIR /build/recipe

COPY compute_wps/ compute_wps/
COPY setup.py .
COPY setup.cfg .
COPY meta.yaml .
COPY pytest.ini .
COPY env .

WORKDIR /build

RUN conda smithy rerender && \
      conda build -c conda-forge -c cdat -m .ci_support/linux_64_.yaml --output-folder channel/ . && \
      conda index channel

FROM $BASE_IMAGE as production

WORKDIR /

COPY --from=builder /build/channel /channel

RUN conda install -c file:///channel -c conda-forge -c cdat compute-wps && \
      conda clean -afy && \
      rm -rf /opt/conda/pkgs && \
      django-admin startproject compute 

WORKDIR /compute

COPY init.sh .
COPY entrypoint.sh .

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

EXPOSE 8000

ARG CONTAINER_IMAGE
ENV CONTAINER_IMAGE $CONTAINER_IMAGE

ENTRYPOINT ["/tini", "--"]

CMD ["./entrypoint.sh"]

FROM scratch as testresult

COPY --from=builder /output/unittest.xml .
COPY --from=builder /output/coverage.xml .
