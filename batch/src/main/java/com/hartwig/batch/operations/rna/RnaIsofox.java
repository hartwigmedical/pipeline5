package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import java.util.List;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.batch.operations.OperationDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class RnaIsofox implements BatchOperation {

    private static final String RNA_BAM_LOCATION = "gs://rna-cohort";
    private static final String ISOFOX_LOCATION = "gs://isofox-resources";
    private static final String ISOFOX_JAR = "isofox.jar";
    private static final String RNA_BAM_FILE_ID = ".sorted.dups.bam";
    private static final String RNA_BAM_INDEX_FILE_ID = ".sorted.dups.bam.bai";
    private static final String ENSEMBL_DATA_CACHE = "ensembl_data_cache";
    private static final String REF_GENOME = "Homo_sapiens.GRCh37.GATK.illumina.fasta";
    private static final String REF_GENOME_INDEX = "Homo_sapiens.GRCh37.GATK.illumina.fasta.fai";
    private static final String EXP_COUNTS_READ_76 = "read_76_exp_rates.csv";
    private static final String EXP_COUNTS_READ_151 = "read_151_exp_rates.csv";
    private static final String READ_LENGTH_76 = "76";
    private static final String READ_LENGTH_151 = "151";

    private static final double GC_BUCKET_76 = 0.0132;
    private static final double GC_BUCKET_151 = 0.0066;
    private static final int FRAG_LENGTH_FRAG_COUNT = 1000000;
    private static final int LONG_FRAG_LENGTH_LIMIT = 550;
    private static final String FRAG_LENGTH_BUCKETS = "50-0;75-0;100-0;125-0;150-0;200-0;250-0;300-0;550-0";
    private static final String ENRICHED_GENE_IDS = " ENSG00000213741;ENSG00000258486;ENSG00000165525;ENSG00000266037;ENSG00000265150;ENSG00000264462;ENSG00000264063";

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String batchInputs = descriptor.value();
        final String[] batchItems = batchInputs.split(",");

        if(batchItems.length != 2)
        {
            System.out.print(String.format("invalid input arguments(%s) - expected SampleId,ReadLength", batchInputs));
            return null;
        }

        final String sampleId = batchItems[0];
        final String readLength = batchItems[1];

        // copy down BAM and index file for this sample
        final String bamFile = String.format("%s%s", sampleId, RNA_BAM_FILE_ID);
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RNA_BAM_LOCATION, sampleId, bamFile, VmDirectories.INPUT));

        final String bamIndexFile = String.format("%s%s", sampleId, RNA_BAM_INDEX_FILE_ID);
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RNA_BAM_LOCATION, sampleId, bamIndexFile, VmDirectories.INPUT));

        // copy down the executable
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                ISOFOX_LOCATION, ISOFOX_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("chmod a+x %s/%s", VmDirectories.TOOLS, ISOFOX_JAR));

        // copy down required reference files
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp -r %s/%s %s",
                ISOFOX_LOCATION, ENSEMBL_DATA_CACHE, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                ISOFOX_LOCATION, REF_GENOME, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                ISOFOX_LOCATION, REF_GENOME_INDEX, VmDirectories.INPUT));

        final String expectedCountsFile = readLength.equals(READ_LENGTH_76) ? EXP_COUNTS_READ_76 : EXP_COUNTS_READ_151;

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                ISOFOX_LOCATION, expectedCountsFile, VmDirectories.INPUT));

        final String threadCount = Bash.allCpus();

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        /*
        gene_transcripts_dir=/data/common/dbs/ensembl_data_cache
        excluded_gene_ids=/data/experiments/rna/blacklist_genes.csv
        ref_genome=/data/common/refgenomes/Homo_sapiens.GRCh37.GATK.illumina/Homo_sapiens.GRCh37.GATK.illumina.fasta
        exp_counts_file=/data/experiments/rna/ref/read_76_exp_rates.csv
        # gene_panel_file=/data/experiments/rna/gene_panel_ids.csv


        while read -r sampleId; do

          input_dir=${input_root}/${sampleId}
          output_dir=${output_root}/${sampleId}
          bam_file=${input_dir}/${sampleId}.sorted.dups.bam

          echo "Running isofox for ${sampleId} on bam: ${bam_file}"

          java -jar ${isofox} -sample ${sampleId} \
            -output_dir ${output_dir} -bam_file ${bam_file} \
            -ref_genome ${ref_genome} \
            -gene_transcripts_dir ${gene_transcripts_dir} \
            -write_frag_lengths -frag_length_min_count 1000000 \
            -long_frag_limit 550 \
            -apply_exp_rates -use_calc_frag_lengths -exp_rate_frag_lengths "50-0;100-0;150-0;200-0;250-0;300-0;550-0" -exp_counts_file ${exp_counts_file} \
            -write_read_gc_ratios -gc_ratio_bucket 0.0132 \
            -threads 8 \
         */

        // run Isofox
        StringBuilder isofoxArgs = new StringBuilder();
        isofoxArgs.append(String.format("-sample %s", sampleId));
        isofoxArgs.append(String.format(" -output_dir %s/", VmDirectories.OUTPUT));
        isofoxArgs.append(String.format(" -bam_file %s/%s", VmDirectories.INPUT, bamFile));
        isofoxArgs.append(String.format(" -ref_genome %s/%s", VmDirectories.INPUT, REF_GENOME));
        isofoxArgs.append(String.format(" -gene_transcripts_dir %s/%s/", VmDirectories.INPUT, ENSEMBL_DATA_CACHE));
        isofoxArgs.append(String.format(" -exp_counts_file %s/%s", VmDirectories.INPUT, expectedCountsFile));
        isofoxArgs.append(String.format(" -write_frag_lengths"));
        isofoxArgs.append(String.format(" -frag_length_min_count %d", FRAG_LENGTH_FRAG_COUNT));
        isofoxArgs.append(String.format(" -long_frag_limit %d", LONG_FRAG_LENGTH_LIMIT));
        isofoxArgs.append(String.format(" -exp_rate_frag_lengths \"%s\" ", FRAG_LENGTH_BUCKETS));
        isofoxArgs.append(String.format(" -apply_exp_rates"));
        isofoxArgs.append(String.format(" -use_calc_frag_lengths"));
        isofoxArgs.append(String.format(" -write_trans_combo_data"));
        isofoxArgs.append(String.format(" -write_exp_rates"));
        isofoxArgs.append(String.format(" -write_read_gc_ratios"));
        isofoxArgs.append(String.format(" -gc_ratio_bucket %f", readLength.equals(READ_LENGTH_76) ? GC_BUCKET_76 : GC_BUCKET_151));
        isofoxArgs.append(String.format(" -enriched_gene_ids \"%s\" ", ENRICHED_GENE_IDS));
        isofoxArgs.append(String.format(" -threads %s", threadCount));

        startupScript.addCommand(() -> format("java -jar %s/%s %s", VmDirectories.TOOLS, ISOFOX_JAR, isofoxArgs.toString()));

        // startupScript.addCommand(new JavaJarCommand("isofox", "2.18.27", "picard.jar", "16G", picargs));

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "isofox"), executionFlags));

        // copy results to rna-analysis location on crunch
        startupScript.addCommand(() -> format("gsutil -m cp %s/* gs://rna-cohort/%s/isofox/", VmDirectories.OUTPUT, sampleId));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-isofox").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(50)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(12, 36)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("RnaIsofox", "Run Isofox RNA analysis",
                OperationDescriptor.InputType.FLAT);
    }


}
