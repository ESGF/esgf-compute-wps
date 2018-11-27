FROM continuumio/miniconda3:4.5.11

RUN git clone https://github.com/nasa-nccs-cds/edask

COPY environment.yaml environment.yaml

RUN conda env create -n edask -f environment.yaml && \
      conda clean -y --all && \
      rm -rf /opt/conda/pkgs/* && \
      cd /edask && \
      PATH=/opt/conda/envs/edask/bin:$PATH python setup.py install && \
      mkdir -p /opt/conda/envs/edask/lib/python3.6/site-packages/resources && \
      touch /opt/conda/envs/edask/lib/python3.6/site-packages/resources/parameters

COPY app.conf /root/.edask/conf/app.conf

ENV UVCDAT_ANONYMOUS_LOG=no

COPY server.py server.py

COPY entrypoint.sh entrypoint.sh

EXPOSE 8786

EXPOSE 8787

ENTRYPOINT ["./entrypoint.sh"]
