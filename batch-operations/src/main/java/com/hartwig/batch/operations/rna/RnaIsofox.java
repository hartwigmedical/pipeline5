package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import static com.hartwig.batch.operations.rna.RnaCommon.MAX_EXPECTED_BAM_SIZE_GB;
import static com.hartwig.batch.operations.rna.RnaCommon.RNA_RESOURCES;
import static com.hartwig.batch.operations.rna.RnaCommon.getRnaCohortDirectory;
import static com.hartwig.batch.operations.rna.RnaCommon.getRnaResourceDirectory;
import static com.hartwig.pipeline.resource.RefGenomeVersion.V37;
import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class RnaIsofox implements BatchOperation {

    private static final String ISOFOX = "isofox";

    private static final String ISOFOX_LOCATION = String.format("%s/%s", RNA_RESOURCES, ISOFOX);

    private static final String ISOFOX_JAR = "isofox.jar";
    private static final String RNA_BAM_FILE_ID = ".sorted.dups.bam";
    private static final String RNA_BAM_INDEX_FILE_ID = ".sorted.dups.bam.bai";
    private static final String KNOWN_FUSIONS_FILE = "known_fusion_data.csv";
    private static final String EXP_COUNTS_READ_76 = "read_76_exp_counts.csv";
    private static final String EXP_COUNTS_READ_151 = "read_151_exp_counts.csv";
    private static final String EXP_GC_COUNTS_READ_100 = "read_100_exp_gc_ratios.csv";
    private static final String NEO_EPITOPE_DIR = "gs://hmf-immune-analysis/neos";
    private static final String READ_LENGTH_76 = "76";
    private static final String READ_LENGTH_151 = "151";

    private static final int FRAG_LENGTH_FRAG_COUNT = 1000000;
    private static final int LONG_FRAG_LENGTH_LIMIT = 550;
    private static final String FRAG_LENGTH_BUCKETS = "50-0;75-0;100-0;125-0;150-0;200-0;250-0;300-0;400-0;550-0";

    public static final String FUNC_TRANSCRIPT_COUNTS = "TRANSCRIPT_COUNTS";
    private static final String FUNC_NOVEL_LOCATIONS = "NOVEL_LOCATIONS";
    private static final String FUNC_FUSIONS = "FUSIONS";
    private static final String FUNC_NEO_EPITOPES = "NEO_EPITOPES";

    private static final int COL_SAMPLE_ID = 0;
    private static final int COL_READ_LENGTH = 1;
    private static final int COL_FUNCTIONS = 2;
    private static final int COL_REF_GENOME_VERSION = 3;
    private static final int COL_MAX_RAM = 4;

    private static final int DEFAULT_MAX_RAM = 64;
    private static final int DEFAULT_CORES = 12;

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String batchInputs = descriptor.inputValue();
        final String[] batchItems = batchInputs.split(",");

        if(batchItems.length < 2)
        {
            System.out.print(String.format("invalid input arguments(%s) - expected SampleId,ReadLength", batchInputs));
            return null;
        }

        final String sampleId = batchItems[COL_SAMPLE_ID];
        final String readLength = batchItems[COL_READ_LENGTH];

        final String functionsStr = batchItems.length > COL_FUNCTIONS
                ? batchItems[COL_FUNCTIONS] : FUNC_TRANSCRIPT_COUNTS + ";" + FUNC_NOVEL_LOCATIONS + ";" + FUNC_FUSIONS;

        final RefGenomeVersion refGenomeVersion = batchItems.length > COL_REF_GENOME_VERSION ?
                RefGenomeVersion.valueOf(batchItems[COL_REF_GENOME_VERSION]) : V37;

        final int maxRam = batchItems.length > COL_MAX_RAM ? Integer.parseInt(batchItems[COL_MAX_RAM]) : DEFAULT_MAX_RAM;

        final ResourceFiles resourceFiles = buildResourceFiles(refGenomeVersion);

        // final String rnaCohortDirectory = getRnaCohortDirectory(refGenomeVersion);
        final String samplesDir = String.format("%s/%s", getRnaCohortDirectory(refGenomeVersion), "samples");

        // copy down BAM and index file for this sample
        final String bamFile = String.format("%s%s", sampleId, RNA_BAM_FILE_ID);
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                samplesDir, sampleId, bamFile, VmDirectories.INPUT));

        final String bamIndexFile = String.format("%s%s", sampleId, RNA_BAM_INDEX_FILE_ID);
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                samplesDir, sampleId, bamIndexFile, VmDirectories.INPUT));

        // copy down the executable
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                ISOFOX_LOCATION, ISOFOX_JAR, VmDirectories.TOOLS));

        // startupScript.addCommand(() -> format("chmod a+x %s/%s", VmDirectories.TOOLS, ISOFOX_JAR));

        // copy down required reference files
        //startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/* %s",
        //        getRnaResourceDirectory(refGenomeVersion, ENSEMBL_DATA_CACHE), VmDirectories.INPUT));

        final String expectedCountsFile = readLength.equals(READ_LENGTH_76) ? EXP_COUNTS_READ_76 : EXP_COUNTS_READ_151;

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/* %s",
                getRnaResourceDirectory(refGenomeVersion, "ensembl_data_cache"), VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                getRnaResourceDirectory(refGenomeVersion, ISOFOX), expectedCountsFile, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                getRnaResourceDirectory(refGenomeVersion, ISOFOX), EXP_GC_COUNTS_READ_100, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                getRnaResourceDirectory(refGenomeVersion, ISOFOX), KNOWN_FUSIONS_FILE, VmDirectories.INPUT));

        final String threadCount = Bash.allCpus();

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        boolean writeExpData = false;
        boolean writeCatCountsData = false;

        final String neoEpitopeFile = String.format("%s.imu.neo_epitopes.csv", sampleId);

        if(functionsStr.contains(FUNC_NEO_EPITOPES)) {

            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                    NEO_EPITOPE_DIR, neoEpitopeFile, VmDirectories.INPUT));
        }

        // run Isofox
        StringBuilder isofoxArgs = new StringBuilder();
        isofoxArgs.append(String.format("-sample %s", sampleId));
        isofoxArgs.append(String.format(" -functions \"%s\"", functionsStr));

        isofoxArgs.append(String.format(" -output_dir %s/", VmDirectories.OUTPUT));
        isofoxArgs.append(String.format(" -bam_file %s/%s", VmDirectories.INPUT, bamFile));

        isofoxArgs.append(String.format(" -ref_genome %s", resourceFiles.refGenomeFile()));

        // current cache has gene data out of order
        isofoxArgs.append(String.format(" -ensembl_data_dir %s", VmDirectories.INPUT));
        // isofoxArgs.append(String.format(" -ensembl_data_dir %s", resourceFiles.ensemblDataCache()));

        isofoxArgs.append(String.format(" -long_frag_limit %d", LONG_FRAG_LENGTH_LIMIT));

        if(refGenomeVersion == RefGenomeVersion.V38)
        {
            isofoxArgs.append(String.format(" -ref_genome_version %s", "38"));
        }

        if(functionsStr.contains(FUNC_TRANSCRIPT_COUNTS))
        {
            isofoxArgs.append(String.format(" -apply_exp_rates"));
            isofoxArgs.append(String.format(" -apply_calc_frag_lengths"));
            isofoxArgs.append(String.format(" -exp_counts_file %s/%s", VmDirectories.INPUT, expectedCountsFile));
            isofoxArgs.append(String.format(" -exp_rate_frag_lengths \"%s\" ", FRAG_LENGTH_BUCKETS));
            isofoxArgs.append(String.format(" -frag_length_min_count %d", FRAG_LENGTH_FRAG_COUNT));

            isofoxArgs.append(String.format(" -apply_gc_bias_adjust"));
            isofoxArgs.append(String.format(" -exp_gc_ratios_file %s/%s", VmDirectories.INPUT, EXP_GC_COUNTS_READ_100));
            isofoxArgs.append(String.format(" -apply_map_qual_adjust"));

            isofoxArgs.append(String.format(" -write_frag_lengths"));
            isofoxArgs.append(String.format(" -write_gc_data"));

            if(writeCatCountsData)
                isofoxArgs.append(String.format(" -write_trans_combo_data"));

            if(writeExpData)
                isofoxArgs.append(String.format(" -write_exp_rates"));
        }

        if(functionsStr.equals(FUNC_NOVEL_LOCATIONS))
        {
            isofoxArgs.append(String.format(" -write_splice_sites"));
        }

        if(functionsStr.equals(FUNC_NEO_EPITOPES))
        {
            isofoxArgs.append(String.format(" -neoepitope_file %s/%s", VmDirectories.INPUT, neoEpitopeFile));
        }

        isofoxArgs.append(String.format(" -known_fusion_file %s/%s", VmDirectories.INPUT, KNOWN_FUSIONS_FILE));

        isofoxArgs.append(String.format(" -threads %s", threadCount));

        startupScript.addCommand(() -> format("java -Xmx60G -jar %s/%s %s", VmDirectories.TOOLS, ISOFOX_JAR, isofoxArgs.toString()));

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "isofox"), executionFlags));

        if(functionsStr.equals(FUNC_FUSIONS))
        {
            startupScript.addCommand(() -> format("gsutil -m cp %s/*fusions.csv %s/%s/isofox/",
                    VmDirectories.OUTPUT, samplesDir, sampleId));
        }
        else
        {
            // copy results to rna-analysis location on crunch
            startupScript.addCommand(() -> format("gsutil -m cp %s/* %s/%s/isofox/", VmDirectories.OUTPUT,
                    samplesDir, sampleId));
        }

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-isofox").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .workingDiskSpaceGb(MAX_EXPECTED_BAM_SIZE_GB)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(DEFAULT_CORES, maxRam)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("RnaIsofox", "Run Isofox RNA analysis",
                OperationDescriptor.InputType.FLAT);
    }

}
