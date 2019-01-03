FROM openjdk:8-jre

ADD bin/janitor.sh janitor.sh
ADD target/lib /usr/share/pipeline5/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/pipeline5/janitor.jar

ENTRYPOINT ["./janitor.sh"]
