#!/bin/bash -x

set -o pipefail

function die() {
  exit_code=$?
  echo "Unknown failure: called command returned $exit_code"
  gsutil -m cp /var/log/run.log gs://run-colo829v003r-colo829v003t-qntrc/gridss
  echo $exit_code > /tmp/JOB_FAILURE
  gsutil -m cp /tmp/JOB_FAILURE gs://run-colo829v003r-colo829v003t-qntrc/gridss
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
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003t-qntrc/aligner/results/COLO829v003T.bam /data/input/COLO829v003T.bam" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003t-qntrc/aligner/results/COLO829v003T.bam /data/input/COLO829v003T.bam >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003t-qntrc/aligner/results/COLO829v003T.bam.bai /data/input/COLO829v003T.bam.bai" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003t-qntrc/aligner/results/COLO829v003T.bam.bai /data/input/COLO829v003T.bam.bai >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qntrc/aligner/results/COLO829v003R.bam /data/input/COLO829v003R.bam" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qntrc/aligner/results/COLO829v003R.bam /data/input/COLO829v003R.bam >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qntrc/aligner/results/COLO829v003R.bam.bai /data/input/COLO829v003R.bam.bai" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qntrc/aligner/results/COLO829v003R.bam.bai /data/input/COLO829v003R.bam.bai >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command ExportPathCommand with bash: export PATH=\"${PATH}:/opt/tools/bwa/0.7.17\"" >>/var/log/run.log 2>&1 || die
export PATH="${PATH}:/opt/tools/bwa/0.7.17" >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command ExportPathCommand with bash: export PATH=\"${PATH}:/opt/tools/samtools/1.14\"" >>/var/log/run.log 2>&1 || die
export PATH="${PATH}:/opt/tools/samtools/1.14" >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command VersionedToolCommand with bash: /opt/tools/gridss/2.13.2/gridss --output /data/output/COLO829v003T.gridss.driver.vcf.gz --assembly /data/output/COLO829v003T.assembly.bam --workingdir /data/output --reference /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta --jar /opt/tools/gridss/2.13.2/gridss.jar --blacklist /opt/resources/gridss_repeatmasker_db/37/ENCFF001TDO.37.bed --configuration /opt/resources/gridss_config/gridss.properties --labels COLO829v003R,COLO829v003T --jvmheap 31G --externalaligner /data/input/COLO829v003R.bam /data/input/COLO829v003T.bam" >>/var/log/run.log 2>&1 || die
/opt/tools/gridss/2.13.2/gridss --output /data/output/COLO829v003T.gridss.driver.vcf.gz --assembly /data/output/COLO829v003T.assembly.bam --workingdir /data/output --reference /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta --jar /opt/tools/gridss/2.13.2/gridss.jar --blacklist /opt/resources/gridss_repeatmasker_db/37/ENCFF001TDO.37.bed --configuration /opt/resources/gridss_config/gridss.properties --labels COLO829v003R,COLO829v003T --jvmheap 31G --externalaligner /data/input/COLO829v003R.bam /data/input/COLO829v003T.bam >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command VersionedToolCommand with bash: /opt/tools/gridss/2.13.2/gridss_annotate_vcf_repeatmasker --output /data/output/COLO829v003T.gridss.repeatmasker.vcf.gz --jar /opt/tools/gridss/2.13.2/gridss.jar -w /data/output --rm /opt/tools/repeatmasker/4.1.1/RepeatMasker /data/output/COLO829v003T.gridss.driver.vcf.gz" >>/var/log/run.log 2>&1 || die
/opt/tools/gridss/2.13.2/gridss_annotate_vcf_repeatmasker --output /data/output/COLO829v003T.gridss.repeatmasker.vcf.gz --jar /opt/tools/gridss/2.13.2/gridss.jar -w /data/output --rm /opt/tools/repeatmasker/4.1.1/RepeatMasker /data/output/COLO829v003T.gridss.driver.vcf.gz >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command AnnotateInsertedSequence with bash: java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.13.2/gridss.jar gridss.AnnotateInsertedSequence REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa INPUT=/data/output/COLO829v003T.gridss.repeatmasker.vcf.gz OUTPUT=/data/output/COLO829v003T.gridss.unfiltered.vcf.gz ALIGNMENT=APPEND WORKER_THREADS=$(grep -c '^processor' /proc/cpuinfo)" >>/var/log/run.log 2>&1 || die
java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.13.2/gridss.jar gridss.AnnotateInsertedSequence REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa INPUT=/data/output/COLO829v003T.gridss.repeatmasker.vcf.gz OUTPUT=/data/output/COLO829v003T.gridss.unfiltered.vcf.gz ALIGNMENT=APPEND WORKER_THREADS=$(grep -c '^processor' /proc/cpuinfo) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command OutputUpload with bash: (cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-colo829v003t-qntrc/gridss/results)" >>/var/log/run.log 2>&1 || die
(cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-colo829v003t-qntrc/gridss/results) >>/var/log/run.log 2>&1 || die
(echo 0 > /tmp/JOB_SUCCESS && gsutil cp /tmp/JOB_SUCCESS gs://run-colo829v003r-colo829v003t-qntrc/gridss) >>/var/log/run.log 2>&1 || die