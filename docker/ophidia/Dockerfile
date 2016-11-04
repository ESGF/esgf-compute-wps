FROM centos:6

RUN yum update -y yum && \
      yum update -y

RUN yum install -y wget

RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

RUN bash Miniconda2-latest-Linux-x86_64.sh -b -f 

ENV PATH=/root/miniconda2/bin:$PATH

RUN conda install -c jasonb857 -c mw -c ostrokach -c conda-forge \
      ophidia-primitives ophidia-analytics-framework ophidia-server 

RUN conda create -n oph-terminal -c jasonb857 -c mw -c ostrokach \
      -c conda-forge ophidia-terminal python=3

RUN yum install -y https://dev.mysql.com/get/mysql57-community-release-el6-7.noarch.rpm 

COPY mysql-community.repo /etc/yum.repos.d/

RUN yum install -y mysql-community-server mysql-community-devel

RUN cp /root/miniconda2/lib/liboph_*.so $(mysql_config --plugindir)

RUN cp /root/miniconda2/lib/libgsl* /lib64

RUN mkdir /root/miniconda2/log

RUN yum install -y libSM libXinerama libXcursor

RUN yum install -y openssh-server

COPY entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

CMD ["source", "activate", "oph-terminal"]
