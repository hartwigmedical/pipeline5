#!/bin/bash -x

set -o pipefail

function die() {
  exit_code=$?
  echo "Unknown failure: called command returned $exit_code"
  gsutil -m cp /var/log/run.log gs://run-colo829v003r-qdvca/cram
  echo $exit_code > /tmp/JOB_FAILURE
  gsutil -m cp /tmp/JOB_FAILURE gs://run-colo829v003r-qdvca/cram
  exit $exit_code
}

mkdir -p /data
mdadm --create /dev/md0 --level=0 --raid-devices=2 /dev/nvme0n1 /dev/nvme0n2
mkfs.ext4 -F /dev/md0
mount /dev/md0 /data
ulimit -n 102400
echo $(date) Starting run >>/var/log/run.log 2>&1 || die
mkdir -p /data/input >>/var/log/run.log 2>&1 || die
mkdir -p /data/output >>/var/log/run.log 2>&1 || die
mkdir -p /data/tmp >>/var/log/run.log 2>&1 || die
export TMPDIR=/data/tmp >>/var/log/run.log 2>&1 || die
export _JAVA_OPTIONS='-Djava.io.tmpdir=/data/tmp' >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qdvca/aligner/results/COLO829v003R.bam /data/input/COLO829v003R.bam" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qdvca/aligner/results/COLO829v003R.bam /data/input/COLO829v003R.bam >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command VersionedToolCommand with bash: /opt/tools/samtools/1.14/samtools view -T /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -o /data/output/COLO829v003R.cram -O cram,embed_ref=1 -@ $(grep -c '^processor' /proc/cpuinfo) /data/input/COLO829v003R.bam" >>/var/log/run.log 2>&1 || die
/opt/tools/samtools/1.14/samtools view -T /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -o /data/output/COLO829v003R.cram -O cram,embed_ref=1 -@ $(grep -c '^processor' /proc/cpuinfo) /data/input/COLO829v003R.bam >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command VersionedToolCommand with bash: /opt/tools/samtools/1.14/samtools index /data/output/COLO829v003R.cram" >>/var/log/run.log 2>&1 || die
/opt/tools/samtools/1.14/samtools index /data/output/COLO829v003R.cram >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command JavaClassCommand with bash: java -Xmx4G -cp /opt/tools/bamcomp/1.3/bamcomp.jar com.hartwig.bamcomp.BamCompMain -r /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -1 /data/input/COLO829v003R.bam -2 /data/output/COLO829v003R.cram -n 6 --samtools-binary /opt/tools/samtools/1.14/samtools --sambamba-binary /opt/tools/sambamba/0.6.8/sambamba" >>/var/log/run.log 2>&1 || die
java -Xmx4G -cp /opt/tools/bamcomp/1.3/bamcomp.jar com.hartwig.bamcomp.BamCompMain -r /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -1 /data/input/COLO829v003R.bam -2 /data/output/COLO829v003R.cram -n 6 --samtools-binary /opt/tools/samtools/1.14/samtools --sambamba-binary /opt/tools/sambamba/0.6.8/sambamba >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command OutputUpload with bash: (cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-qdvca/cram/results)" >>/var/log/run.log 2>&1 || die
(cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-qdvca/cram/results) >>/var/log/run.log 2>&1 || die
(echo 0 > /tmp/JOB_SUCCESS && gsutil cp /tmp/JOB_SUCCESS gs://run-colo829v003r-qdvca/cram) >>/var/log/run.log 2>&1 || die