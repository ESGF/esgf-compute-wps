FROM continuumio/miniconda:4.2.12

COPY requirements.txt requirements.txt

RUN conda install -c conda-forge pyzmq gunicorn lxml && \
	pip install -r requirements.txt

RUN apt-get update --fix-missing && \
      apt-get install -y --no-install-recommends git libpq-dev gcc && \
      git clone https://github.com/esgf/esgf-compute-api && \
      cd esgf-compute-api && \
      git checkout 2.0 && \
      pip install -e . && \
      apt-get clean -y

COPY . /var/www

WORKDIR /var/www/compute

CMD ["gunicorn", "-b", "0.0.0.0:8000", "--reload", "compute.wsgi"]
