#!/bin/bash -x

set -o pipefail

function die() {
  exit_code=$?
  echo "Unknown failure: called command returned $exit_code"
  gsutil -m cp /var/log/run.log gs://run-colo829v003r-colo829v003t-qntrc/gripss_germline
  echo $exit_code > /tmp/JOB_FAILURE
  gsutil -m cp /tmp/JOB_FAILURE gs://run-colo829v003r-colo829v003t-qntrc/gripss_germline
  exit $exit_code
}

mkdir -p /data
mdadm --create /dev/md0 --level=0 --raid-devices=4 /dev/nvme0n1 /dev/nvme0n2 /dev/nvme0n3 /dev/nvme0n4
mkfs.ext4 -F /dev/md0
mount /dev/md0 /data
ulimit -n 102400
echo $(date) Starting run >>/var/log/run.log 2>&1 || die
mkdir -p /data/input >>/var/log/run.log 2>&1 || die
mkdir -p /data/output >>/var/log/run.log 2>&1 || die
mkdir -p /data/tmp >>/var/log/run.log 2>&1 || die
export TMPDIR=/data/tmp >>/var/log/run.log 2>&1 || die
export _JAVA_OPTIONS='-Djava.io.tmpdir=/data/tmp' >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-colo829v003t-qntrc/gridss/results/COLO829v003T.gridss.unfiltered.vcf.gz /data/input/COLO829v003T.gridss.unfiltered.vcf.gz" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-colo829v003t-qntrc/gridss/results/COLO829v003T.gridss.unfiltered.vcf.gz /data/input/COLO829v003T.gridss.unfiltered.vcf.gz >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-colo829v003t-qntrc/gridss/results/COLO829v003T.gridss.unfiltered.vcf.gz.tbi /data/input/COLO829v003T.gridss.unfiltered.vcf.gz.tbi" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-colo829v003t-qntrc/gridss/results/COLO829v003T.gridss.unfiltered.vcf.gz.tbi /data/input/COLO829v003T.gridss.unfiltered.vcf.gz.tbi >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command JavaJarCommand with bash: java -Xmx16G -jar /opt/tools/gripss/2.0/gripss.jar -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -known_hotspot_file /opt/resources/fusions/37/known_fusions.37.bedpe -pon_sgl_file /opt/resources/gridss_pon/37/gridss_pon_single_breakend.37.bed -pon_sv_file /opt/resources/gridss_pon/37/gridss_pon_breakpoint.37.bedpe -output_id germline -sample COLO829v003R -vcf /data/input/COLO829v003T.gridss.unfiltered.vcf.gz -output_dir /data/output" >>/var/log/run.log 2>&1 || die
java -Xmx16G -jar /opt/tools/gripss/2.0/gripss.jar -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -known_hotspot_file /opt/resources/fusions/37/known_fusions.37.bedpe -pon_sgl_file /opt/resources/gridss_pon/37/gridss_pon_single_breakend.37.bed -pon_sv_file /opt/resources/gridss_pon/37/gridss_pon_breakpoint.37.bedpe -output_id germline -sample COLO829v003R -vcf /data/input/COLO829v003T.gridss.unfiltered.vcf.gz -output_dir /data/output >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command OutputUpload with bash: (cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-colo829v003t-qntrc/gripss_germline/results)" >>/var/log/run.log 2>&1 || die
(cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-colo829v003t-qntrc/gripss_germline/results) >>/var/log/run.log 2>&1 || die
(echo 0 > /tmp/JOB_SUCCESS && gsutil cp /tmp/JOB_SUCCESS gs://run-colo829v003r-colo829v003t-qntrc/gripss_germline) >>/var/log/run.log 2>&1 || die