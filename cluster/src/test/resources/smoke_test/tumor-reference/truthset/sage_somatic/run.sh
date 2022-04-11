#!/bin/bash -x

set -o pipefail

function die() {
  exit_code=$?
  echo "Unknown failure: called command returned $exit_code"
  gsutil -m cp /var/log/run.log gs://run-colo829v003r-colo829v003t-qdvca/sage_somatic
  echo $exit_code > /tmp/JOB_FAILURE
  gsutil -m cp /tmp/JOB_FAILURE gs://run-colo829v003r-colo829v003t-qdvca/sage_somatic
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
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003t-qdvca/aligner/results/COLO829v003T.bam /data/input/COLO829v003T.bam" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003t-qdvca/aligner/results/COLO829v003T.bam /data/input/COLO829v003T.bam >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003t-qdvca/aligner/results/COLO829v003T.bam.bai /data/input/COLO829v003T.bam.bai" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003t-qdvca/aligner/results/COLO829v003T.bam.bai /data/input/COLO829v003T.bam.bai >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qdvca/aligner/results/COLO829v003R.bam /data/input/COLO829v003R.bam" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qdvca/aligner/results/COLO829v003R.bam /data/input/COLO829v003R.bam >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command InputDownload with bash: gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qdvca/aligner/results/COLO829v003R.bam.bai /data/input/COLO829v003R.bam.bai" >>/var/log/run.log 2>&1 || die
gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://run-colo829v003r-qdvca/aligner/results/COLO829v003R.bam.bai /data/input/COLO829v003R.bam.bai >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command JavaJarCommand with bash: java -Xmx60G -jar /opt/tools/sage/pilot/sage.jar -tumor COLO829v003T -tumor_bam /data/input/COLO829v003T.bam -reference COLO829v003R -reference_bam /data/input/COLO829v003R.bam -hotspots /opt/resources/sage/37/KnownHotspots.somatic.37.vcf.gz -panel_bed /opt/resources/sage/37/ActionableCodingPanel.somatic.37.bed.gz -coverage_bed /opt/resources/sage/37/ActionableCodingPanel.somatic.37.bed.gz -high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -ref_genome_version V37 -ensembl_data_dir /opt/resources/ensembl_data_cache/37/ -write_bqr_data -write_bqr_plot -out /data/output/COLO829v003T.sage.somatic.vcf.gz -threads $(grep -c '^processor' /proc/cpuinfo)" >>/var/log/run.log 2>&1 || die
java -Xmx60G -jar /opt/tools/sage/pilot/sage.jar -tumor COLO829v003T -tumor_bam /data/input/COLO829v003T.bam -reference COLO829v003R -reference_bam /data/input/COLO829v003R.bam -hotspots /opt/resources/sage/37/KnownHotspots.somatic.37.vcf.gz -panel_bed /opt/resources/sage/37/ActionableCodingPanel.somatic.37.bed.gz -coverage_bed /opt/resources/sage/37/ActionableCodingPanel.somatic.37.bed.gz -high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -ref_genome_version V37 -ensembl_data_dir /opt/resources/ensembl_data_cache/37/ -write_bqr_data -write_bqr_plot -out /data/output/COLO829v003T.sage.somatic.vcf.gz -threads $(grep -c '^processor' /proc/cpuinfo) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command PipeCommands with bash: (/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/COLO829v003T.sage.somatic.vcf.gz -O z -o /data/output/COLO829v003T.sage.somatic.filtered.vcf.gz)" >>/var/log/run.log 2>&1 || die
(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER="PASS"' /data/output/COLO829v003T.sage.somatic.vcf.gz -O z -o /data/output/COLO829v003T.sage.somatic.filtered.vcf.gz) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command TabixCommand with bash: /opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.sage.somatic.filtered.vcf.gz -p vcf" >>/var/log/run.log 2>&1 || die
/opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.sage.somatic.filtered.vcf.gz -p vcf >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command OutputUpload with bash: (cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-colo829v003t-qdvca/sage_somatic/results)" >>/var/log/run.log 2>&1 || die
(cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-colo829v003t-qdvca/sage_somatic/results) >>/var/log/run.log 2>&1 || die
(echo 0 > /tmp/JOB_SUCCESS && gsutil cp /tmp/JOB_SUCCESS gs://run-colo829v003r-colo829v003t-qdvca/sage_somatic) >>/var/log/run.log 2>&1 || die