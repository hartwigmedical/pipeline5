FROM google/cloud-sdk:slim

RUN apt-get update
RUN apt-get --yes install openjdk-11-jre

ADD docker_run.sh docker_run.sh
ADD target/lib /usr/share/hartwig/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/hartwig/bootstrap.jar

ENTRYPOINT ["./docker_run.sh"]
