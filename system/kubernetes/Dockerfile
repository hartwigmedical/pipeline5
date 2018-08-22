FROM google/cloud-sdk:alpine

# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

# add a simple script that can auto-detect the appropriate JAVA_HOME value
# based on whether the JDK or only the JRE is installed
RUN { \
		echo '#!/bin/sh'; \
		echo 'set -e'; \
		echo; \
		echo 'dirname "$(dirname "$(readlink -f "$(which javac || which java)")")"'; \
	} > /usr/local/bin/docker-java-home \
	&& chmod +x /usr/local/bin/docker-java-home
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk
ENV PATH $PATH:/usr/lib/jvm/java-1.8-openjdk/jre/bin:/usr/lib/jvm/java-1.8-openjdk/bin

ENV JAVA_VERSION 8u171
ENV JAVA_ALPINE_VERSION 8.171.11-r0

RUN set -x \
	&& apk add --no-cache \
		openjdk8="$JAVA_ALPINE_VERSION" \
	&& [ "$JAVA_HOME" = "$(docker-java-home)" ]

ADD https://storage.googleapis.com/kubernetes-release/release/v1.6.4/bin/linux/amd64/kubectl /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl

ARG BASE_DIR=patient-cluster

ADD ${BASE_DIR}/init-cluster.sh /
ADD ${BASE_DIR}/k8-submit.sh /
ADD ${BASE_DIR}/namenode.yaml.template /
ADD ${BASE_DIR}/datanode.yaml /
ADD ${BASE_DIR}/init-cluster.sh /
ADD spark-2.3.0-bin-hadoop2.7 /spark-2.3.0-bin-hadoop2.7
RUN chmod +x /init-cluster.sh
RUN chmod +x /k8-submit.sh

ENTRYPOINT ["/init-cluster.sh"]