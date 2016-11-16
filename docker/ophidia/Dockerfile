FROM centos:6.7

RUN yum update -y yum && \
      yum update -y

RUN yum install -y \
      http://dev.mysql.com/get/mysql57-community-release-el6-7.noarch.rpm

RUN yum install -y yum-utils

RUN yum-config-manager --disable mysql57-community && \
      yum-config-manager --enable mysql56-community

RUN yum install -y mysql-community-server wget tar bzip2 \
      libXrender libSM libXext libXinerama libXcursor

RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

RUN bash Miniconda2-latest-Linux-x86_64.sh -f -b

ENV PATH=/root/miniconda2/bin:/root/miniconda2/sbin:$PATH

RUN conda install -c jasonb857 -c conda-forge -c mw \
      ophidia-primitives ophidia-analytics-framework \
      ophidia-server ophidia-terminal slurm

RUN yum install -y mysql-community-devel

RUN cp /root/miniconda2/lib/liboph_*.so $(mysql_config --plugindir)

RUN cp /root/miniconda2/lib/libgsl* /lib64 && \
      cp /root/miniconda2/lib/libmatheval* /lib64

COPY entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
