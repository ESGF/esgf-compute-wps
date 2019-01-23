FROM continuumio/miniconda3:4.5.11

ARG BRANCH=master

RUN git clone https://github.com/nasa-nccs-cds/edask && \
      cd edask && \
      git checkout ${BRANCH}

RUN conda create -n edask -c conda-forge python=3.6 bokeh bottleneck dask dateparser defusedxml \
      distributed eofs keras libnetcdf matplotlib netCDF4 nco paramiko pillow pydap pyparsing \
      pytest python-graphviz pyzmq scikit-learn scipy xarray zeromq cartopy && \
      conda clean -y --all && \
      rm -rf /opt/conda/pkgs/*
      
RUN PATH=/opt/conda/envs/edask/bin:$PATH pip install sklearn_xarray

RUN cd /edask && \
      PATH=/opt/conda/envs/edask/bin:$PATH python setup.py install && \
      mkdir -p /opt/conda/envs/edask/lib/python3.6/site-packages/resources && \
      touch /opt/conda/envs/edask/lib/python3.6/site-packages/resources/parameters

COPY docker/edask/app.conf /root/.edask/conf/app.conf

ENV UVCDAT_ANONYMOUS_LOG=no

COPY docker/edask/server.py server.py

COPY docker/edask/entrypoint.sh entrypoint.sh

EXPOSE 8786

EXPOSE 8787

ENTRYPOINT ["./entrypoint.sh"]
