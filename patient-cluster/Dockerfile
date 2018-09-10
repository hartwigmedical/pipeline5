FROM openjdk:8-jre

ADD bin/bootstrap.sh bootstrap.sh
ADD target/lib /usr/share/pipeline5/lib
ARG SYSTEM_JAR_FILE
ARG JAR_FILE
ADD target/lib/${SYSTEM_JAR_FILE} /usr/share/pipeline5/system.jar
ADD target/${JAR_FILE} /usr/share/pipeline5/bootstrap.jar

ENTRYPOINT ["./bootstrap.sh"]
