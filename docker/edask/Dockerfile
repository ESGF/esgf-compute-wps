FROM continuumio/miniconda:4.3.27

RUN git clone https://github.com/nasa-nccs-cds/edask

COPY environment.yaml environment.yaml

RUN conda env create -n edask -f environment.yaml && \
      conda clean -y --all && \
      rm -rf /opt/conda/pkgs/* && \
      cd /edask && \
      PATH=/opt/conda/envs/edask/bin:$PATH python setup.py install && \
      mkdir -p /opt/conda/envs/edask/lib/python3.6/site-packages/resources && \
      touch /opt/conda/envs/edask/lib/python3.6/site-packages/resources/parameters

ENV UVCDAT_ANONYMOUS_LOG=no

ENV EDAS_CACHE_DIR=/data/edas

COPY server.py server.py

COPY entrypoint.sh entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
