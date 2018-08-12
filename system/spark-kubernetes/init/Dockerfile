FROM google/cloud-sdk:alpine

ADD scripts/download-patient-data.sh /
ADD keys/gcloud-read-credentials.json /
RUN chmod +x /download-patient-data.sh