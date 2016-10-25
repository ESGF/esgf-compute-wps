FROM centos:6

RUN yum update -y yum
RUN yum update -y
RUN yum install -y epel-release
RUN yum groupinstall -y 'development tools'

RUN yum install -y https://dev.mysql.com/get/mysql57-community-release-el6-7.noarch.rpm 

COPY mysql-community.repo /etc/yum.repos.d/

RUN yum install -y git gsl gsl-devel wget guile-devel flex-devel zlib-devel \
	pkgconfig mysql-community-server mysql-community-devel \
	libtool-ltdl-devel mpich mpich-devel libxml2-devel openssl-devel \
	jansson-devel libcurl-devel libssh2-devel graphviz-devel gtk2-devel \
	ncurses-devel readline-devel

RUN git clone https://github.com/OphidiaBigData/ophidia-primitives
RUN git clone https://github.com/OphidiaBigData/ophidia-analytics-framework
RUN git clone https://github.com/OphidiaBigData/ophidia-server 
RUN git clone https://github.com/OphidiaBigData/ophidia-terminal

RUN wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.8.16/src/hdf5-1.8.16.tar.gz && \
	tar -xvf hdf5-1.8.16.tar.gz && \
	cd hdf5-1.8.16 && \
	CC=/usr/lib64/mpich/bin/mpicc ./configure --prefix=/usr/local/ophidia/extra --enable-parallel && \
	make -j6 && \
	make install 

RUN wget https://github.com/Unidata/netcdf-c/archive/v4.4.0.tar.gz && \
	tar -xvf v4.4.0.tar.gz && \
	cd netcdf-c-4.4.0 && \
	CC=/usr/lib64/mpich/bin/mpicc CPPFLAGS="-I/usr/local/ophidia/extra/include" LDFLAGS="-L/usr/local/ophidia/extra/lib" LIBS=-ldl ./configure --prefix=/usr/local/ophidia/extra --enable-parallel-tests && \
	make -j6 && \
	make install

RUN wget http://ftp.gnu.org/gnu/libmatheval/libmatheval-1.1.11.tar.gz && \
	tar -xvf libmatheval-1.1.11.tar.gz && \
	cd libmatheval-1.1.11 && \
	./configure --prefix=/usr/local/ophidia/extra && \
	make -j6 && \
	make install

RUN wget http://downloads.sourceforge.net/project/gsoap2/gsoap-2.8/gsoap_2.8.27.zip && \
	unzip gsoap_2.8.27 && \
	cd gsoap-2.8 && \
	./configure --prefix=/usr/local/ophidia/extra && \
	make && \
	make install

RUN cd ophidia-primitives && \
	./bootstrap && \
	PKG_CONFIG_PATH=/usr/local/ophidia/extra/lib/pkgconfig ./configure --prefix=/usr/local/ophidia/oph-cluster/oph-primitives --with-matheval-path=/usr/local/ophidia/extra/lib && \
	make -j6 && \
	make install 

RUN cp /usr/local/ophidia/oph-cluster/oph-primitives/lib/liboph_*.so $(mysql_config --plugindir)

ENV CC=/usr/lib64/mpich/bin/mpicc

RUN cd ophidia-analytics-framework && \
	./bootstrap && \
	./configure --prefix=/usr/local/ophidia/oph-cluster/oph-analytics-framework --enable-parallel-netcdf --with-netcdf-path=/usr/local/ophidia/extra/ --with-web-server-path=/var/www/html/ophidia --with-web-server-url=http://127.0.0.1/ophidia && \
	make -j6 && \
	make install

RUN cd ophidia-server && \
	echo "AC_SUBST([AM_DEFAULT_VERBOSITY], [1])" >> configure.ac && \
	./bootstrap && \
	PKG_CONFIG_PATH=/usr/local/ophidia/extra/lib/pkgconfig ./configure --prefix=/usr/local/ophidia/oph-server --with-framework-path=/usr/local/ophidia/oph-cluster/oph-analytics-framework --with-soapcpp2-path=/usr/local/ophidia/extra --enable-webaccess --with-web-server-path=/var/www/html/ophidia --with-web-server-url=http://127.0.0.1/ophidia && \
	make && \
	make install

RUN cp -r ophidia-server/authz /usr/local/ophidia/oph-server && \
	mkdir -p /usr/local/ophidia/oph-server/authz/sessions

RUN cd ophidia-terminal && \
	./bootstrap && \
	./configure --prefix=/usr/local/ophidia/oph-terminal && \
	make -j6 && \
	make install

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

RUN git clone git://github.com/SchedMD/slurm.git && \
	cd slurm && \
	git checkout tags/slurm-14-11-3-1 && \
	./configure --prefix=/usr/local/ophidia/extra && \
	make -j6 && \
	make install

RUN yum install -y httpd php mod_ssl openssl openssh-server

COPY entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

EXPOSE 22 80 443 11732

ENTRYPOINT ["./entrypoint.sh"]

CMD ["/usr/local/ophidia/oph-server/bin/oph_server", "-d"]
