FROM ubuntu:24.04

ARG TIMEZONE_VAR="America/Los_Angeles"

ENV container=docker

RUN echo root:cbdb@123 | chpasswd && \
    apt update -qq && \
    apt -o Dpkg::Progress-Fancy=0 \
         install -y -qq \
         systemd \
         systemd-sysv \
         dbus \
         procps \
         openssh-server && \
    apt -o Dpkg::Progress-Fancy=0 \
         install -y -qq \
         bison \
         curl \
         flex \
         g++-11 \
         gcc-11 \
         git \
         iproute2 \
         iputils-ping \
         libapr1-dev \
         libbz2-dev \
         libcurl4-openssl-dev \
         libevent-dev \
         libipc-run-perl \
         libjansson-dev \
         libkrb5-dev \
         libldap-dev \
         libpam-dev \
         libperl-dev \
         libreadline-dev \
         libssl-dev \
         libtest-harness-perl \
         libtest-simple-perl \
         libuv1-dev \
         libxerces-c-dev \
         libxml2-dev \
         libyaml-dev \
         libzstd-dev \
         make \
         pkg-config \
         python3-dev \
         python3-psutil \
         python3-pygresql \
         rsync \
         vim \
         wget \
         zlib1g-dev && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100 && \
    update-alternatives --install /usr/bin/x86_64-linux-gnu-gcc x86_64-linux-gnu-gcc /usr/bin/gcc-11 200

RUN apt -o Dpkg::Progress-Fancy=0 \
    install -y -qq \
         bat \
         htop \
         silversearcher-ag \
         sudo \
         tmux && \
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
                --without-pythonsrc-ext                        \
                                                            && \
    make -j$(nproc)                                         && \
    make install                                            && \
    rm -rf /tmp/cloudberrydb

COPY ./configs/* /tmp/

RUN cp -v /tmp/90-cbdb-sysctl.conf /etc/sysctl.d && \
    RAM_IN_KB=`cat /proc/meminfo | grep MemTotal | awk '{print $2}'`; \
    RAM_IN_BYTES=$(($RAM_IN_KB*1024)); \
    echo "vm.min_free_kbytes = $(($RAM_IN_BYTES*3/100/1024))" >> /etc/sysctl.d/90-cbdb-sysctl.conf ; \
    echo "kernel.shmall = $(($RAM_IN_BYTES/2/4096))" >> /etc/sysctl.d/90-cbdb-sysctl.conf ; \
    echo "kernel.shmmax = $(($RAM_IN_BYT    ES/2))" >> /etc/sysctl.d/90-cbdb-sysctl.conf ; \
    if [ $RAM_IN_BYTES -le $((64*1024*1024*1024)) ]; \
    then echo "vm.dirty_background_ratio = 3" >> /etc/sysctl.d/90-cbdb-sysctl.conf ; \
         echo "vm.dirty_ratio = 10" >> /etc/sysctl.d/90-cbdb-sysctl.conf \
    else echo "vm.dirty_background_ratio = 0" >> /etc/sysctl.d/90-cbdb-sysctl.conf ; \
         echo "vm.dirty_ratio = 0" >> /etc/sysctl.d/90-cbdb-sysctl.conf ; \
         echo "vm.dirty_background_bytes = 1610612736 # 1.5GB" >> /etc/sysctl.d/90-cbdb-sysctl.conf ; \
         echo "vm.dirty_bytes = 4294967296 # 4GB" >> /etc/sysctl.d/90-cbdb-sysctl.conf ; \
    fi

RUN cp /tmp/90-cbdb-limits.conf /etc/security/limits.d/90-cbdb-limits.conf && \
    cat /usr/share/zoneinfo/${TIMEZONE_VAR} > /etc/localtime && \
    echo "cdw" > /tmp/gpdb-segment-hosts && \
    chmod a+r /tmp/gpinitsystem_singlenode && \
    chmod a+x /tmp/init_system.sh && \
    hostname > ~/orig_hostname && \
    /usr/sbin/groupadd gpadmin && \
    /usr/sbin/useradd  gpadmin -m -s /usr/bin/bash -g gpadmin && \
    echo "gpadmin:cbdb@123" | chpasswd && \
    echo "gpadmin        ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    echo "root           ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    echo "export COORDINATOR_DATA_DIRECTORY=/data0/database/coordinator/gpseg-1" >> /home/gpadmin/.bashrc && \
    echo "source /usr/local/cloudberry-db/greenplum_path.sh"                     >> /home/gpadmin/.bashrc && \
    mkdir -p /data0/database/coordinator \
             /data0/database/standby_coordinator \
             /data0/database/primary \
             /data0/database/mirror && \
    chown -R gpadmin:gpadmin /data0 && \
    ssh-keygen -A && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config

# Create necessary directories and files for systemd to work properly
RUN mkdir -p /run/systemd && echo 'docker' > /run/systemd/container

RUN mkdir /var/run/sshd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/UsePAM yes/UsePAM no/' /etc/ssh/sshd_config

EXPOSE 5432 22

VOLUME [ "/sys/fs/cgroup" ]
CMD ["bash","-c","/tmp/init_system.sh"]
