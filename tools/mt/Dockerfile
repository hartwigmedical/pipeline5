# step 1: check Bash script with shellcheck
FROM koalaman/shellcheck-alpine:stable AS build
ADD src/extract-mt.sh .
RUN /bin/shellcheck extract-mt.sh

# step 2: copy checked script into final image
FROM staphb/samtools:1.20

SHELL ["bash", "-c"]
WORKDIR /usr/local/bin/

COPY --from=build extract-mt.sh .
RUN chmod +x extract-mt.sh

ENTRYPOINT ["/usr/local/bin/extract-mt.sh"]
