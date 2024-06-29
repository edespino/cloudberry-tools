FROM almalinux:9

ARG TIMEZONE_VAR="America/Los_Angeles"

ENV container=docker

RUN dnf update -y && \
    dnf install -y systemd \
                   systemd-libs && \
    dnf clean all

# Clean up unnecessary systemd units
RUN [ -d /lib/systemd/system/sysinit.target.wants ] && find /lib/systemd/system/sysinit.target.wants/ -type l -not -name 'systemd-tmpfiles-setup.service' -delete || echo "Directory /lib/systemd/system/sysinit.target.wants does not exist" && \
    [ -d /lib/systemd/system/multi-user.target.wants ] && find /lib/systemd/system/multi-user.target.wants/ -type l -delete || echo "Directory /lib/systemd/system/multi-user.target.wants does not exist" && \
    [ -d /etc/systemd/system/*.wants ] && find /etc/systemd/system/*.wants/ -type l -delete || echo "Directory /etc/systemd/system/*.wants does not exist" && \
    [ -d /lib/systemd/system/local-fs.target.wants ] && find /lib/systemd/system/local-fs.target.wants/ -type l -delete || echo "Directory /lib/systemd/system/local-fs.target.wants does not exist" && \
    [ -d /lib/systemd/system/sockets.target.wants ] && find /lib/systemd/system/sockets.target.wants/ -type l -not -name '*udev*' -delete || echo "Directory /lib/systemd/system/sockets.target.wants does not exist" && \
    [ -d /lib/systemd/system/basic.target.wants ] && find /lib/systemd/system/basic.target.wants/ -type l -delete || echo "Directory /lib/systemd/system/basic.target.wants does not exist" && \
    [ -d /lib/systemd/system/anaconda.target.wants ] && find /lib/systemd/system/anaconda.target.wants/ -type l -delete || echo "Directory /lib/systemd/system/anaconda.target.wants does not exist"

RUN echo root:cbdb@123 | chpasswd && \
    dnf makecache && \
    dnf install -y yum-utils \
                   epel-release \
                   git && \
    dnf makecache && \
    yum-config-manager --disable epel && \
    dnf install -y --enablerepo=epel \
                   the_silver_searcher \
                   bat \
                   htop && \
    dnf install -y bison \
                   cmake3 \
                   ed \
                   flex \
                   gcc \
                   gcc-c++ \
                   glibc-langpack-en \
                   initscripts \
                   iproute \
                   less \
                   m4 \
                   net-tools \
                   openssh-clients \
                   openssh-server \
                   passwd \
                   perl \
                   rsync \
                   sudo \
                   tar \
                   unzip \
                   util-linux-ng \
                   wget \
                   which && \
    dnf install -y apr-devel \
                   bzip2-devel \
                   krb5-devel \
                   libcurl-devel \
                   libevent-devel \
                   libxml2-devel \
                   libzstd-devel \
                   openldap-devel \
                   openssl-devel \
                   pam-devel \
                   perl-ExtUtils-Embed \
                   perl-Test-Simple \
                   perl-core \
                   python3-devel \
                   readline-devel \
                   zlib-devel && \
    dnf install -y --enablerepo=crb \
                   libuv-devel \
                   libyaml-devel \
                   perl-IPC-Run && \
    dnf install -y --enablerepo=epel \
                   xerces-c-devel && \
    wget "https://go.dev/dl/$(curl -Ls https://go.dev/VERSION?m=text | head -n 1).linux-amd64.tar.gz" &&  \
    tar xf go*.linux-amd64.tar.gz    && \
    cp go/bin/* /usr/bin             && \
    rm -rf go go*.linux-amd64.tar.gz

RUN git clone --branch main                                    \
              --single-branch                                  \
              --depth=1                                        \
              https://github.com/cloudberrydb/cloudberrydb.git \
              /tmp/cloudberrydb                                \
                                                            && \
    cd /tmp/cloudberrydb                                    && \
    ./configure --prefix=/usr/local/cloudberry-db              \
                --enable-cassert                               \
                --enable-debug-extensions                      \
                --enable-ic-proxy                              \
                --enable-mapreduce                             \
                --enable-orafce                                \
                --enable-orca                                  \
                --enable-pxf                                   \
                --enable-tap-tests                             \
                --with-gssapi                                  \
                --with-ldap                                    \
                --with-libxml                                  \
                --with-openssl                                 \
                --with-pam                                     \
                --with-perl                                    \
                --with-pgport=5432                             \
                --with-python                                  \
                --with-pythonsrc-ext                           \
                                                            && \
    make -j$(nproc)                                         && \
    make install                                            && \
    rm -rf /tmp/cloudberrydb

COPY ./configs/* /tmp/

RUN cp -v /tmp/90-cbdb-sysctl.conf /etc/sysctl.conf && \
    RAM_IN_KB=`cat /proc/meminfo | grep MemTotal | awk '{print $2}'`; \
    RAM_IN_BYTES=$(($RAM_IN_KB*1024)); \
    echo "vm.min_free_kbytes = $(($RAM_IN_BYTES*3/100/1024))" >> /etc/sysctl.conf ; \
    echo "kernel.shmall = $(($RAM_IN_BYTES/2/4096))" >> /etc/sysctl.conf ; \
    echo "kernel.shmmax = $(($RAM_IN_BYT    ES/2))" >> /etc/sysctl.conf ; \
    if [ $RAM_IN_BYTES -le $((64*1024*1024*1024)) ]; \
    then echo "vm.dirty_background_ratio = 3" >> /etc/sysctl.conf ; \
         echo "vm.dirty_ratio = 10" >> /etc/sysctl.conf \
    else echo "vm.dirty_background_ratio = 0" >> /etc/sysctl.conf ; \
         echo "vm.dirty_ratio = 0" >> /etc/sysctl.conf ; \
         echo "vm.dirty_background_bytes = 1610612736 # 1.5GB" >> /etc/sysctl.conf ; \
         echo "vm.dirty_bytes = 4294967296 # 4GB" >> /etc/sysctl.conf ; \
    fi

RUN cp /tmp/90-cbdb-limits.conf /etc/security/limits.d/90-cbdb-limits.conf && \
    cat /usr/share/zoneinfo/${TIMEZONE_VAR} > /etc/localtime && \
    echo "cdw" > /tmp/gpdb-segment-hosts && \
    echo "/usr/local/lib" >> /etc/ld.so.conf && \
    echo "/usr/local/lib64" >> /etc/ld.so.conf && \
    ldconfig && \
    chmod a+r /tmp/gpinitsystem_singlenode && \
    chmod a+x /tmp/init_system.sh && \
    hostname > ~/orig_hostname && \
    /usr/sbin/groupadd gpadmin && \
    /usr/sbin/useradd  gpadmin -g gpadmin -G wheel && \
    echo "cbdb@123"|passwd --stdin gpadmin && \
    echo "gpadmin        ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    echo "root           ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    echo "export COORDINATOR_DATA_DIRECTORY=/data0/database/coordinator/gpseg-1" >> /home/gpadmin/.bashrc && \
    echo "source /usr/local/cloudberry-db/greenplum_path.sh"                >> /home/gpadmin/.bashrc && \
    mkdir -p /data0/database/coordinator \
             /data0/database/standby_coordinator \
             /data0/database/primary \
             /data0/database/mirror && \
    chown -R gpadmin:gpadmin /data0 && \
    ssh-keygen -A && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config

EXPOSE 5432 22

VOLUME [ "/sys/fs/cgroup" ]
CMD ["bash","-c","/tmp/init_system.sh"]
