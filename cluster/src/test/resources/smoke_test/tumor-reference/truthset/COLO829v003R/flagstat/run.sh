#!/bin/bash -x

set -o pipefail

function die() {
  exit_code=$?
  echo "Unknown failure: called command returned $exit_code"
  gsutil -m cp /var/log/run.log gs://run-colo829v003r-qntrc/flagstat
  echo $exit_code > /tmp/JOB_FAILURE
  gsutil -m cp /tmp/JOB_FAILURE gs://run-colo829v003r-qntrc/flagstat
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
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qntrc/aligner/results/COLO829v003R.bam /data/input/COLO829v003R.bam" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qntrc/aligner/results/COLO829v003R.bam /data/input/COLO829v003R.bam >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command SubShellCommand with bash: (/opt/tools/sambamba/0.6.8/sambamba flagstat -t $(grep -c '^processor' /proc/cpuinfo) /data/input/COLO829v003R.bam > /data/output/COLO829v003R.flagstat)" >>/var/log/run.log 2>&1 || die
(/opt/tools/sambamba/0.6.8/sambamba flagstat -t $(grep -c '^processor' /proc/cpuinfo) /data/input/COLO829v003R.bam > /data/output/COLO829v003R.flagstat) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command OutputUpload with bash: (cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-qntrc/flagstat/results)" >>/var/log/run.log 2>&1 || die
(cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-qntrc/flagstat/results) >>/var/log/run.log 2>&1 || die
(echo 0 > /tmp/JOB_SUCCESS && gsutil cp /tmp/JOB_SUCCESS gs://run-colo829v003r-qntrc/flagstat) >>/var/log/run.log 2>&1 || die