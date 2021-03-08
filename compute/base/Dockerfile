ARG BASE_IMAGE
FROM ${BASE_IMAGE} as production

ENV USER esgf
ENV UID 1000
ENV GID 1000

RUN groupadd -g ${GID} conda && \
      useradd -m -u ${UID} -g conda ${USER}

WORKDIR /home/esgf

RUN chown -R root:conda /opt/conda && \
      chmod -R 775 /opt/conda && \ 
      chown -R esgf:conda /home/esgf

USER $USER

RUN mamba install -n base -c conda-forge -y boa && \
      conda clean -afy && \
      rm -rf /opt/conda/pkgs
