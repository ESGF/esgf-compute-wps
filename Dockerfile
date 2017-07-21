FROM continuumio/miniconda:4.2.12

COPY . /var/www

WORKDIR /var/www

RUN conda install -c conda-forge -c uvcdat esgf-compute-api cdms2 cdutil \
	genutil pyzmq gunicorn lxml  && \
	pip install -r requirements.txt

RUN apt-get update --fix-missing && \
      apt-get install -y --no-install-recommends git libpq-dev gcc && \
      apt-get clean -y

WORKDIR /var/www/compute

EXPOSE 8000

CMD ["gunicorn", "-b", "0.0.0.0:8000", "--reload", "compute.wsgi"]
