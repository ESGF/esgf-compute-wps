FROM ubuntu:trusty

RUN apt-get update

RUN apt-get install -y curl git build-essential libssl-dev libffi-dev

RUN curl -o conda.sh https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

RUN bash conda.sh -b -f

ENV PATH=/root/miniconda2/bin:$PATH

RUN conda install -c uvcdat -y cdms2 genutil cdutil lxml gdal

COPY requirements.txt requirements.txt

RUN C_INCLUDE_PATH=/usr/include/gdal CPLUS_INCLUDE_PATH=/usr/include/gdal pip install -r requirements.txt

WORKDIR /var/www

COPY . .

RUN mkdir -p /tmp/wps /data

COPY entrypoint.sh entrypoint.sh

RUN chmod 0755 entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

CMD ["python", "compute/manage.py", "runserver", "0.0.0.0:8000"]
