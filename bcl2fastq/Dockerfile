FROM google/cloud-sdk:slim

RUN apt-get update
RUN apt-get --yes install openjdk-11-jre
RUN dpkg -i rclone-current-linux-amd64.deb


ADD bin/bcl2fastq.sh bcl2fastq.sh
ADD target/lib /usr/share/bcl2fastq/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/bcl2fastq/bcl2fastq.jar

ENTRYPOINT ["./bcl2fastq.sh"]
