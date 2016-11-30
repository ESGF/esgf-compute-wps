FROM ubuntu:trusty

RUN apt-get update && \
      apt-get install -y curl git build-essential libssl-dev libffi-dev

RUN curl -o conda.sh https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh && \
      bash conda.sh -b -f

ENV PATH=/root/miniconda2/bin:$PATH

RUN conda install -c conda-forge -c uvcdat cdat_info=2.6 cdms2=2.6 cdtime=2.6 \
      cdutil=2.6 distarray=2.6 genutil=2.6 unidata=2.6 lxml gdal=1.11.2 \
      libgdal=1.11.2 owslib

#RUN ln -sf /root/miniconda2/lib/libnetcdf.so.7 /root/miniconda2/lib/libnetcdf.so.11

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN conda install -y -c conda-forge -c uvcdat/label/nightly -c uvcdat vcs-nox

WORKDIR /var/www

COPY . .

COPY entrypoint.sh entrypoint.sh

ENV HDF5_DISABLE_VERSION_CHECK=1

ENV UVCDAT_ANONYMOUS_LOG=yes

RUN vcs_download_sample_data && \
      mkdir -p /tmp/wps /data && \
      cp /root/miniconda2/share/uvcdat/sample_data/* /data

ENTRYPOINT ["./entrypoint.sh"]

CMD ["python", "compute/manage.py", "runserver", "0.0.0.0:8000"]
