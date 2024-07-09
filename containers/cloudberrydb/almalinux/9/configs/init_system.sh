#!/bin/bash
## ======================================================================
## Container initialization script
## ======================================================================

## Start SSH daemon and setup for ssh access
/usr/sbin/sshd

rm -rf /run/nologin

echo $(grep $(hostname) /etc/hosts | cut -f1) cdw >> /etc/hosts
echo "127.0.0.1 $(cat ~/orig_hostname)" >> /etc/hosts

## Set gpadmin ownership - Clouberry install directory and supporting
## cluster creation files.
chown -R gpadmin:gpadmin /usr/local/cloudberrydb \
                         /tmp/gpinitsystem_singlenode \
                         /tmp/gpdb-segment-hosts

# Allow passwordless ssh access
su gpadmin -l \
           -c "mkdir -p /home/gpadmin/.ssh; chmod 700 /home/gpadmin/.ssh; \
               ssh-keygen -t rsa -b 4096 -C gpadmin -f /home/gpadmin/.ssh/id_rsa -P \"\" > /dev/null 2>&1; \
               cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys; \
               chmod 600 /home/gpadmin/.ssh/authorized_keys; \
               ssh-keyscan -t rsa cdw > /home/gpadmin/.ssh/known_hosts; \
               ssh cdw uptime"

cat <<'EOF'

======================================================================
  ____ _                 _ _                            ____  ____
 / ___| | ___  _   _  __| | |__   ___ _ __ _ __ _   _  |  _ \| __ )
| |   | |/ _ \| | | |/ _` | '_ \ / _ \ '__| '__| | | | | | | |  _ \
| |___| | (_) | |_| | (_| | |_) |  __/ |  | |  | |_| | | |_| | |_) |
 \____|_|\___/ \__,_|\__,_|_.__/ \___|_|  |_|   \__, | |____/|____/
                                                |___/
======================================================================

EOF

# Initialize Cloudberry cluster
su - gpadmin -l \
             -c "source /usr/local/cloudberrydb/greenplum_path.sh; \
                 gpinitsystem -a \
                              -c /tmp/gpinitsystem_singlenode \
                              -h /tmp/gpdb-segment-hosts \
                              --max_connections=100"

## Allow any host access the Cloudberry Cluster
su - gpadmin -l \
             -c "source /usr/local/cloudberrydb/greenplum_path.sh; \
                 export COORDINATOR_DATA_DIRECTORY=/data0/database/coordinator/gpseg-1; \
                 echo 'host     all         all             0.0.0.0/0           trust' >> /data0/database/coordinator/gpseg-1/pg_hba.conf; \
                 gpstop -u"

su - gpadmin -l \
             -c "source /usr/local/cloudberrydb/greenplum_path.sh; \
                 psql -P pager=off -d template1 \
                      -c \"ALTER USER gpadmin PASSWORD 'cbdb@123'\""

cat <<'EOF'

======================================================================
Sandbox: Cloudberry Database Cluster details
======================================================================

EOF

echo "Current time: $(date)"
source /etc/os-release
echo "OS Version: ${NAME} ${VERSION}"

## Set gpadmin password, display version and cluster configuration
su - gpadmin -l \
             -c "source /usr/local/cloudberrydb/greenplum_path.sh; \
                 psql -P pager=off -d template1 \
                      -c \"SELECT VERSION()\"; \
                 psql -P pager=off -d template1 \
                      -c \"SELECT * FROM gp_segment_configuration\"; \
                 psql -P pager=off -d template1 \
                      -c \"SHOW optimizer\""

/bin/bash
