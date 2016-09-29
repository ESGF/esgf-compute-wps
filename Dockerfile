FROM ubuntu:trusty

RUN apt-get update

RUN apt-get install -y curl build-essential libcpl-dev libgdal-dev libxml2-dev libxslt1-dev git

RUN curl -o conda.sh https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

RUN bash conda.sh -b -f

ENV PATH=/root/miniconda2/bin:$PATH

RUN conda install -c uvcdat -y uvcdat

COPY requirements.txt requirements.txt

RUN pip install lxml==3.5.0

RUN C_INCLUDE_PATH=/usr/include/gdal CPLUS_INCLUDE_PATH=/usr/include/gdal pip install -r requirements.txt --no-deps

WORKDIR /var/www

COPY compute/ compute/

WORKDIR compute/

RUN python manage.py migrate

RUN mkdir -p /tmp/wps /data

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
