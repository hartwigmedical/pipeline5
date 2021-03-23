FROM google/cloud-sdk:latest

RUN apt-get update
RUN apt-get --yes install openjdk-11-jre
RUN curl https://downloads.rclone.org/v1.48.0/rclone-v1.48.0-linux-amd64.deb --output rclone-current-linux-amd64.deb
RUN dpkg -i rclone-current-linux-amd64.deb


ADD bin/pipeline5.sh pipeline5.sh
ADD target/lib /usr/share/pipeline5/lib
ARG VERSION
ADD target/cluster-local-SNAPSHOT.jar /usr/share/pipeline5/bootstrap.jar

ENTRYPOINT ["./pipeline5.sh"]