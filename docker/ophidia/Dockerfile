FROM ubuntu:trusty

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get autoclean && \
      apt-get update

RUN apt-get install -y autoconf autotools-dev gcc make git graphviz-dev \
      gsl-bin libgsl0-dev libgtk2.0-dev  apache2-bin php5 libapache2-mod-php5 \
      libjansson-dev libxml2 libxml2-dev openssh-server openssl libssl-dev \
      libssh2-1-dev libreadline-dev libltdl-dev mpich libmpich2-dev curl \
      libcurl3 libcurl4-openssl-dev

RUN apt-get install -y mysql-server-5.6  mysql-client-5.6 libmysqlclient-dev \
      munge libmunge-dev libmunge2

RUN apt-get install -y wget

RUN wget --no-check-certificate \
  https://download.ophidia.cmcc.it/deb/0.10/ophidia-analytics-framework_0.10.7-0_amd64.deb && \
  wget --no-check-certificate \
  https://download.ophidia.cmcc.it/deb/0.10/ophidia-primitives_0.10.6-0_amd64.deb && \
  wget --no-check-certificate \
  https://download.ophidia.cmcc.it/deb/0.10/ophidia-server_0.10.7-0_amd64.deb && \
  wget --no-check-certificate \
  https://download.ophidia.cmcc.it/deb/0.10/ophidia-terminal_0.10.6-0_amd64.deb

RUN dpkg -i ophidia-primitives_0.10.*_amd64.deb && \
      dpkg -i ophidia-analytics-framework_0.10.*_amd64.deb && \
      dpkg -i ophidia-server_0.10.*_amd64.deb && \
      dpkg -i ophidia-terminal_0.10.*_amd64.deb

RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh && \
      bash Miniconda2-latest-Linux-x86_64.sh -f -b

ENV PATH=/root/miniconda2/bin:/root/miniconda2/sbin:$PATH

RUN conda install -y -c jasonb857 slurm

RUN wget https://ftp.gnu.org/gnu/libmatheval/libmatheval-1.1.10.tar.gz

RUN apt-get install -y guile-1.8-dev libfl-dev flex bison

RUN tar -xvf libmatheval-1.1.10.tar.gz && \
      cd libmatheval-1.1.10 && \
      ./configure --prefix=/usr && \
      make && \
      make install

RUN wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.8.16/src/hdf5-1.8.16.tar.gz

RUN tar -xvf hdf5-1.8.16.tar.gz && \
      cd hdf5-1.8.16 && \
      CC=/usr/bin/mpicc ./configure --prefix=/usr/local/ophidia/extra --enable-parallel && \
      make && \
      make install

RUN wget https://github.com/Unidata/netcdf-c/archive/v4.4.0.tar.gz

RUN tar -xvf v4.4.0.tar.gz && \
      cd netcdf-c-4.4.0 && \
      CC=/usr/bin/mpicc CPPFLAGS="-I/usr/local/ophidia/extra/include" LDFLAGS="-L/usr/local/ophidia/extra/lib" LIBS="-ldl" ./configure --prefix=/usr/local/ophidia/extra && \
      make && \
      make install

RUN cp /usr/local/ophidia/oph-cluster/oph-primitives/lib/liboph_*.so /usr/lib/mysql/plugin

COPY entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

CMD ["oph_term", "-H", "127.0.0.1", "-P", "11732", "-u", "oph-test", "-p", "abcd"]
