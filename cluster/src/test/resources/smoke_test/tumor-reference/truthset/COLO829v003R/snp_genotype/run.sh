#!/bin/bash -x

set -o pipefail

function die() {
  exit_code=$?
  echo "Unknown failure: called command returned $exit_code"
  gsutil -m cp /var/log/run.log gs://run-colo829v003r-qntrc/snp_genotype
  echo $exit_code > /tmp/JOB_FAILURE
  gsutil -m cp /tmp/JOB_FAILURE gs://run-colo829v003r-qntrc/snp_genotype
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
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qntrc/aligner/results/COLO829v003R.bam.bai /data/input/COLO829v003R.bam.bai" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qntrc/aligner/results/COLO829v003R.bam.bai /data/input/COLO829v003R.bam.bai >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command SnpGenotypeCommand with bash: /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T UnifiedGenotyper -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/COLO829v003R.bam -o /data/output/snp_genotype_output.vcf -L /opt/resources/genotype_snps/37/26SNPtaq.vcf --reference_sequence /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta --output_mode EMIT_ALL_SITES" >>/var/log/run.log 2>&1 || die
/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T UnifiedGenotyper -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/COLO829v003R.bam -o /data/output/snp_genotype_output.vcf -L /opt/resources/genotype_snps/37/26SNPtaq.vcf --reference_sequence /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta --output_mode EMIT_ALL_SITES >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command OutputUpload with bash: (cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-qntrc/snp_genotype/results)" >>/var/log/run.log 2>&1 || die
(cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-qntrc/snp_genotype/results) >>/var/log/run.log 2>&1 || die
(echo 0 > /tmp/JOB_SUCCESS && gsutil cp /tmp/JOB_SUCCESS gs://run-colo829v003r-qntrc/snp_genotype) >>/var/log/run.log 2>&1 || die