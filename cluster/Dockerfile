FROM google/cloud-sdk:slim

RUN apt-get update
RUN apt-get --yes install openjdk-8-jre
RUN curl https://downloads.rclone.org/rclone-current-linux-amd64.deb  --output rclone-current-linux-amd64.deb
RUN dpkg -i rclone-current-linux-amd64.deb


ADD bin/pipeline5.sh pipeline5.sh
ADD src/main/resources/node-init.sh node-init.sh
ADD target/lib /usr/share/pipeline5/lib
ARG SYSTEM_JAR_FILE
ARG JAR_FILE
ADD target/lib/${SYSTEM_JAR_FILE} /usr/share/pipeline5/system.jar
ADD target/${JAR_FILE} /usr/share/pipeline5/bootstrap.jar

ENTRYPOINT ["./pipeline5.sh"]
