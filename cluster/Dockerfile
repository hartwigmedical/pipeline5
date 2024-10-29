FROM google/cloud-sdk:latest

RUN apt-get update
RUN apt-get --yes install openjdk-17-jre

ADD bin/pipeline5.sh pipeline5.sh
ADD target/lib /usr/share/pipeline5/lib
ARG VERSION
ADD target/cluster-$VERSION.jar /usr/share/pipeline5/bootstrap.jar

ENTRYPOINT ["./pipeline5.sh"]
