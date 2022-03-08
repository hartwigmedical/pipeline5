package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import static com.hartwig.batch.operations.rna.RnaCommon.MAX_EXPECTED_BAM_SIZE_GB;
import static com.hartwig.batch.operations.rna.RnaCommon.RNA_RESOURCES;
import static com.hartwig.batch.operations.rna.RnaCommon.getRnaCohortDirectory;
import static com.hartwig.batch.operations.rna.RnaCommon.getRnaResourceDirectory;
import static com.hartwig.pipeline.resource.RefGenomeVersion.V37;
import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.util.StringJoiner;

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

    public static final String ISOFOX = "isofox";
    public static final String ISOFOX_LOCATION = String.format("%s/%s", RNA_RESOURCES, ISOFOX);
    public static final String ISOFOX_JAR = "isofox.jar";
    public static final String RNA_BAM_FILE_ID = ".sorted.dups.bam";
    public static final String RNA_BAM_INDEX_FILE_ID = ".sorted.dups.bam.bai";

    private static final String KNOWN_FUSIONS_FILE = "known_fusion_data.csv";
    private static final String EXP_COUNTS_READ_76 = "read_76_exp_counts.csv";
    private static final String EXP_COUNTS_READ_151 = "read_151_exp_counts.csv";
    private static final String EXP_GC_COUNTS_READ_100 = "read_100_exp_gc_ratios.csv";
    private static final String COHORT_FUSION_FILE = "hmf_isofox_fusion_cohort.csv";
    private static final String NEO_EPITOPE_DIR = "gs://hmf-immune-analysis/neos";
    private static final String READ_LENGTH_76 = "76";
    private static final String READ_LENGTH_151 = "151";

    private static final int FRAG_LENGTH_FRAG_COUNT = 1000000;
    private static final int LONG_FRAG_LENGTH_LIMIT = 550;

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
            final InputBundle inputs, final RuntimeBucket bucket, final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

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

        if(functionsStr.contains(FUNC_TRANSCRIPT_COUNTS))
        {
            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                    getRnaResourceDirectory(refGenomeVersion, ISOFOX), expectedCountsFile, VmDirectories.INPUT));

            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                    getRnaResourceDirectory(refGenomeVersion, ISOFOX), EXP_GC_COUNTS_READ_100, VmDirectories.INPUT));
        }

        if(functionsStr.equals(FUNC_FUSIONS))
        {
            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                    getRnaResourceDirectory(refGenomeVersion, ISOFOX), COHORT_FUSION_FILE, VmDirectories.INPUT));
        }

        final String threadCount = Bash.allCpus();

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        boolean writeExpData = false;
        boolean writeCatCountsData = false;

        final String neoEpitopeFile = String.format("%s.imu.neo_epitopes.csv", sampleId);

        if(functionsStr.contains(FUNC_NEO_EPITOPES))
        {
            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                    NEO_EPITOPE_DIR, neoEpitopeFile, VmDirectories.INPUT));
        }

        // run Isofox
        StringJoiner isofoxArgs = new StringJoiner(" ");
        isofoxArgs.add(String.format("-sample %s", sampleId));
        isofoxArgs.add(String.format("-functions \"%s\"", functionsStr));

        isofoxArgs.add(String.format("-output_dir %s/", VmDirectories.OUTPUT));
        isofoxArgs.add(String.format("-bam_file %s/%s", VmDirectories.INPUT, bamFile));

        isofoxArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));

        // isofoxArgs.add(String.format("-ensembl_data_dir %s", VmDirectories.INPUT));
        isofoxArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));

        isofoxArgs.add(String.format("-long_frag_limit %d", LONG_FRAG_LENGTH_LIMIT));

        if(refGenomeVersion == RefGenomeVersion.V38)
        {
            isofoxArgs.add(String.format("-ref_genome_version %s", "38"));
        }

        if(functionsStr.contains(FUNC_TRANSCRIPT_COUNTS))
        {
            isofoxArgs.add(String.format("-apply_exp_rates"));
            isofoxArgs.add(String.format("-apply_calc_frag_lengths"));
            isofoxArgs.add(String.format("-exp_counts_file %s/%s", VmDirectories.INPUT, expectedCountsFile));
            isofoxArgs.add(String.format("-frag_length_min_count %d", FRAG_LENGTH_FRAG_COUNT));

            isofoxArgs.add(String.format("-apply_gc_bias_adjust"));
            isofoxArgs.add(String.format("-exp_gc_ratios_file %s/%s", VmDirectories.INPUT, EXP_GC_COUNTS_READ_100));
            isofoxArgs.add(String.format("-apply_map_qual_adjust"));

            isofoxArgs.add(String.format("-write_frag_lengths"));
            isofoxArgs.add(String.format("-write_gc_data"));

            if(writeCatCountsData)
                isofoxArgs.add(String.format("-write_trans_combo_data"));

            if(writeExpData)
                isofoxArgs.add(String.format("-write_exp_rates"));
        }

        if(functionsStr.equals(FUNC_NOVEL_LOCATIONS))
        {
            isofoxArgs.add(String.format("-write_splice_sites"));
        }

        if(functionsStr.contains(FUNC_FUSIONS))
        {
            isofoxArgs.add(String.format("-known_fusion_file %s", resourceFiles.knownFusionData()));
            isofoxArgs.add(String.format("-fusion_cohort_file %s/%s", VmDirectories.INPUT, COHORT_FUSION_FILE));
        }

        if(functionsStr.equals(FUNC_NEO_EPITOPES))
        {
            isofoxArgs.add(String.format("-neoepitope_file %s/%s", VmDirectories.INPUT, neoEpitopeFile));
        }

        isofoxArgs.add(String.format("-threads %s", threadCount));

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
