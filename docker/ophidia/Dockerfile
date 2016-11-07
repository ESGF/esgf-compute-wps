FROM centos:6

RUN yum update -y yum && \
      yum update -y

RUN yum install -y https://dev.mysql.com/get/mysql57-community-release-el6-7.noarch.rpm 

COPY mysql-community.repo /etc/yum.repos.d/

RUN yum install -y wget mysql-community-server mysql-community-devel \
      libSM libXinerama libXcursor openssh-server epel-release

RUN yum install -y munge munge-devel && \
      dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key && \
      mkdir /var/run/munge && \
      chmod 0400 /etc/munge/munge.key && \
      chmod 0700 /var/lib/munge && \
      chmod 0711 /var/log/munge && \
      chmod 0755 /var/run/munge && \
      chown -R munge:munge /etc/munge && \
      chown -R munge:munge /var/lib/munge && \
      chown -R munge:munge /var/log/munge && \
      chown -R munge:munge /var/run/munge

RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh

RUN bash Miniconda2-latest-Linux-x86_64.sh -b -f 

ENV PATH=/root/miniconda2/bin:/root/miniconda2/sbin:$PATH

RUN yum install -y glibc-devel openssh-clients

RUN conda install -c jasonb857 -c conda-forge \
      ophidia-primitives ophidia-analytics-framework ophidia-server \
      slurm

RUN conda create -n oph-term -c jasonb857 -c mw -c ostrokach -c conda-forge \
      ophidia-terminal python=3

RUN cp /root/miniconda2/lib/liboph_*.so $(mysql_config --plugindir)
RUN cp /root/miniconda2/lib/libgsl* /lib64

RUN mkdir -p /root/miniconda2/log
RUN mkdir -p /root/miniconda2/txt
RUN mkdir -p /root/miniconda2/authz/sessions
RUN mkdir -p /var/www/html/sessions

RUN sed -ibak 's/\(SUBM_CMD\).*/\1=\/root\/miniconda2\/bin\/srun/g' /root/miniconda2/etc/rmanager.conf
RUN sed -ibak 's/\(SUBM_CANCEL\).*/\1=\/root\/miniconda2\/bin\/scancel -n/g' /root/miniconda2/etc/rmanager.conf
RUN sed -ibak 's/\(SUBM_JOBCHECK\).*/\1=\/root\/miniconda2\/bin\/squeue -o "%j" | grep oph/g' /root/miniconda2/etc/rmanager.conf

COPY entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

EXPOSE 22 80 443 11732

ENTRYPOINT ["./entrypoint.sh"]

COPY oph_term.sh oph_term.sh

RUN chmod +x oph_term.sh

CMD ["bash", "oph_term.sh"]
