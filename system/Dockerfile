FROM openjdk:8-jre

RUN apt-get update
RUN apt-get install -y bwa

ENTRYPOINT ["./pipeline.sh"]

ADD bin/pipeline.sh pipeline.sh
ADD docker/conf/pipeline.yaml /conf/pipeline.yaml
ADD target/lib /usr/share/pipeline2/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/pipeline2/system.jar
