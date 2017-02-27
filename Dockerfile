FROM continuumio/miniconda:4.2.12

RUN apt-get update --fix-missing && \
      apt-get install -y --no-install-recommends git && \
      git clone https://github.com/esgf/esgf-compute-api && \
      cd esgf-compute-api && \
      git checkout 2.0 && \
      pip install -e . && \
      apt-get clean -y

ENV PATH=/opt/conda/bin:$PATH

RUN conda install -c conda-forge pyzmq gunicorn redis && \
	pip install django celery

COPY . /var/www

WORKDIR /var/www/compute

CMD ["gunicorn", "-b", "0.0.0.0:8000", "--reload", "compute.wsgi"]
