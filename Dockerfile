FROM ubuntu:trusty

RUN apt-get update

RUN apt-get install -y curl git build-essential libssl-dev libffi-dev

RUN curl -o conda.sh https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

RUN bash conda.sh -b -f

ENV PATH=/root/miniconda2/bin:$PATH

RUN conda install -c uvcdat -c conda-forge cdat_info=2.6 cdms2=2.6 cdtime=2.6 \
      cdutil=2.6 distarray=2.6 genutil=2.6 unidata=2.6 lxml gdal=1.11.2 \
      libgdal=1.11.2

COPY requirements.txt requirements.txt

RUN C_INCLUDE_PATH=/usr/include CPLUS_INCLUDE_PATH=/usr/include pip install -r requirements.txt

RUN ln -sf /root/miniconda2/lib/libnetcdf.so.7 /root/miniconda2/lib/libnetcdf.so.11

WORKDIR /var/www

COPY . .

RUN mkdir -p /tmp/wps /data

COPY entrypoint.sh /entrypoint.sh

RUN chmod 0755 /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

CMD ["python", "compute/manage.py", "runserver", "0.0.0.0:8000"]
