FROM ubuntu:latest

RUN apt-get update && \
      apt-get install -y wget bzip2 git

RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O conda.sh && \
      bash conda.sh -bfp /opt/conda

ENV PATH=/opt/conda/bin:$PATH

RUN conda install -c conda-forge -c uvcdat uvcdat

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN git clone https://github.com/nasa-nccs-cds/CDAS2 && \
	cd CDAS2 && \
	python setup.py install

RUN mkdir -p /root/.cdas

WORKDIR /var/www

COPY . .

RUN sed -i 's/^ALLOWED_HOSTS.*/ALLOWED_HOSTS=\["0.0.0.0"\]/' /var/www/compute/compute/settings.py

ENV UVCDAT_ANONYMOUS_LOG=yes

WORKDIR /var/www/compute

CMD ["gunicorn", "-b", "0.0.0.0:8000", "compute.wsgi"]
#CMD ["python", "compute/manage.py", "runserver", "0.0.0.0:8000"]
