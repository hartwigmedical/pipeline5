#!/bin/bash -x

set -o pipefail

function die() {
  exit_code=$?
  echo "Unknown failure: called command returned $exit_code"
  gsutil -m cp /var/log/run.log gs://run-colo829v003r-colo829v003t-qntrc/sage_germline
  echo $exit_code > /tmp/JOB_FAILURE
  gsutil -m cp /tmp/JOB_FAILURE gs://run-colo829v003r-colo829v003t-qntrc/sage_germline
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
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command SageCommand with bash: java -Xmx15G -cp /opt/tools/sage/2.8/sage.jar com.hartwig.hmftools.sage.SageApplication -tumor COLO829v003R -tumor_bam /data/input/COLO829v003R.bam -reference COLO829v003T -reference_bam /data/input/COLO829v003T.bam -hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz -panel_bed /opt/resources/sage/37/ActionableCodingPanel.germline.37.bed.gz -hotspot_min_tumor_qual 50 -panel_min_tumor_qual 75 -hotspot_max_germline_vaf 100 -hotspot_max_germline_rel_raw_base_qual 100 -panel_max_germline_vaf 100 -panel_max_germline_rel_raw_base_qual 100 -mnv_filter_enabled false -high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -out /data/output/COLO829v003T.sage.germline.vcf.gz -assembly hg19 -threads $(grep -c '^processor' /proc/cpuinfo) -panel_only -coverage_bed /opt/resources/sage/37/CoverageCodingPanel.germline.37.bed.gz" >>/var/log/run.log 2>&1 || die
java -Xmx15G -cp /opt/tools/sage/2.8/sage.jar com.hartwig.hmftools.sage.SageApplication -tumor COLO829v003R -tumor_bam /data/input/COLO829v003R.bam -reference COLO829v003T -reference_bam /data/input/COLO829v003T.bam -hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz -panel_bed /opt/resources/sage/37/ActionableCodingPanel.germline.37.bed.gz -hotspot_min_tumor_qual 50 -panel_min_tumor_qual 75 -hotspot_max_germline_vaf 100 -hotspot_max_germline_rel_raw_base_qual 100 -panel_max_germline_vaf 100 -panel_max_germline_rel_raw_base_qual 100 -mnv_filter_enabled false -high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -out /data/output/COLO829v003T.sage.germline.vcf.gz -assembly hg19 -threads $(grep -c '^processor' /proc/cpuinfo) -panel_only -coverage_bed /opt/resources/sage/37/CoverageCodingPanel.germline.37.bed.gz >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command PipeCommands with bash: (/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/COLO829v003T.sage.germline.vcf.gz -O z -o /data/output/COLO829v003T.sage.pass.vcf.gz)" >>/var/log/run.log 2>&1 || die
(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER="PASS"' /data/output/COLO829v003T.sage.germline.vcf.gz -O z -o /data/output/COLO829v003T.sage.pass.vcf.gz) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command TabixCommand with bash: /opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.sage.pass.vcf.gz -p vcf" >>/var/log/run.log 2>&1 || die
/opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.sage.pass.vcf.gz -p vcf >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command PipeCommands with bash: (/opt/tools/bcftools/1.9/bcftools view -s COLO829v003R,COLO829v003T /data/output/COLO829v003T.sage.pass.vcf.gz -O z -o /data/output/COLO829v003T.sage.sort.vcf.gz)" >>/var/log/run.log 2>&1 || die
(/opt/tools/bcftools/1.9/bcftools view -s COLO829v003R,COLO829v003T /data/output/COLO829v003T.sage.pass.vcf.gz -O z -o /data/output/COLO829v003T.sage.sort.vcf.gz) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command TabixCommand with bash: /opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.sage.sort.vcf.gz -p vcf" >>/var/log/run.log 2>&1 || die
/opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.sage.sort.vcf.gz -p vcf >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command PipeCommands with bash: (/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/mappability/37/out_150.mappability.37.bed.gz -h /opt/resources/mappability/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/COLO829v003T.sage.sort.vcf.gz -O z -o /data/output/COLO829v003T.mappability.annotated.vcf.gz)" >>/var/log/run.log 2>&1 || die
(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/mappability/37/out_150.mappability.37.bed.gz -h /opt/resources/mappability/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/COLO829v003T.sage.sort.vcf.gz -O z -o /data/output/COLO829v003T.mappability.annotated.vcf.gz) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command TabixCommand with bash: /opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.mappability.annotated.vcf.gz -p vcf" >>/var/log/run.log 2>&1 || die
/opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.mappability.annotated.vcf.gz -p vcf >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command PipeCommands with bash: (/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/clinvar.37.vcf.gz -c INFO/CLNSIG,INFO/CLNSIGCONF /data/output/COLO829v003T.mappability.annotated.vcf.gz -O z -o /data/output/COLO829v003T.clinvar.vcf.gz)" >>/var/log/run.log 2>&1 || die
(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/clinvar.37.vcf.gz -c INFO/CLNSIG,INFO/CLNSIGCONF /data/output/COLO829v003T.mappability.annotated.vcf.gz -O z -o /data/output/COLO829v003T.clinvar.vcf.gz) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command TabixCommand with bash: /opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.clinvar.vcf.gz -p vcf" >>/var/log/run.log 2>&1 || die
/opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.clinvar.vcf.gz -p vcf >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command PipeCommands with bash: (/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/KnownBlacklist.germline.37.bed.gz -m BLACKLIST_BED -c CHROM,FROM,TO /data/output/COLO829v003T.clinvar.vcf.gz -O z -o /data/output/COLO829v003T.blacklist.regions.vcf.gz)" >>/var/log/run.log 2>&1 || die
(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/KnownBlacklist.germline.37.bed.gz -m BLACKLIST_BED -c CHROM,FROM,TO /data/output/COLO829v003T.clinvar.vcf.gz -O z -o /data/output/COLO829v003T.blacklist.regions.vcf.gz) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command TabixCommand with bash: /opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.blacklist.regions.vcf.gz -p vcf" >>/var/log/run.log 2>&1 || die
/opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.blacklist.regions.vcf.gz -p vcf >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command PipeCommands with bash: (/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/KnownBlacklist.germline.37.vcf.gz -m BLACKLIST_VCF /data/output/COLO829v003T.blacklist.regions.vcf.gz -O z -o /data/output/COLO829v003T.sage.germline.filtered.vcf.gz)" >>/var/log/run.log 2>&1 || die
(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/KnownBlacklist.germline.37.vcf.gz -m BLACKLIST_VCF /data/output/COLO829v003T.blacklist.regions.vcf.gz -O z -o /data/output/COLO829v003T.sage.germline.filtered.vcf.gz) >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command TabixCommand with bash: /opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.sage.germline.filtered.vcf.gz -p vcf" >>/var/log/run.log 2>&1 || die
/opt/tools/tabix/0.2.6/tabix /data/output/COLO829v003T.sage.germline.filtered.vcf.gz -p vcf >>/var/log/run.log 2>&1 || die
echo $(date "+%Y-%m-%d %H:%M:%S") "Running command OutputUpload with bash: (cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-colo829v003t-qntrc/sage_germline/results)" >>/var/log/run.log 2>&1 || die
(cp /var/log/run.log /data/output && gsutil -qm rsync -r /data/output/ gs://run-colo829v003r-colo829v003t-qntrc/sage_germline/results) >>/var/log/run.log 2>&1 || die
(echo 0 > /tmp/JOB_SUCCESS && gsutil cp /tmp/JOB_SUCCESS gs://run-colo829v003r-colo829v003t-qntrc/sage_germline) >>/var/log/run.log 2>&1 || die