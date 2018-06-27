FROM openjdk:8-jre

RUN apt-get update
RUN apt-get install -y build-essential gcc-multilib apt-utils zlib1g-dev wget

RUN apt-get install -y git
WORKDIR /tmp
RUN git clone https://github.com/lh3/bwa.git
WORKDIR /tmp/bwa
ARG BWA_VERSION
RUN git checkout ${BWA_VERSION}
RUN make
RUN cp -p bwa /usr/local/bin
RUN rm -rf /tmp/bwa
RUN apt-get clean
RUN apt-get remove -y --purge build-essential gcc-multilib apt-utils zlib1g-dev wget
WORKDIR /

ENTRYPOINT ["./pipeline.sh"]

ADD bin/pipeline.sh pipeline.sh
ADD docker/conf/pipeline.yaml /conf/pipeline.yaml
ADD target/lib /usr/share/pipeline2/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/pipeline2/system.jar
