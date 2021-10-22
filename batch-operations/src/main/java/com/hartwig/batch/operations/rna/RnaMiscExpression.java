package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import static com.hartwig.batch.operations.rna.RnaCommon.MAX_EXPECTED_BAM_SIZE_GB;
import static com.hartwig.batch.operations.rna.RnaCommon.RNA_RESOURCES;
import static com.hartwig.batch.operations.rna.RnaCommon.getRnaCohortDirectory;
import static com.hartwig.batch.operations.rna.RnaCommon.getRnaResourceDirectory;
import static com.hartwig.batch.operations.rna.RnaIsofox.FUNC_TRANSCRIPT_COUNTS;
import static com.hartwig.pipeline.resource.RefGenomeVersion.V37;
import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.util.StringJoiner;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
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
import com.hartwig.pipeline.tools.Versions;

public class RnaMiscExpression implements BatchOperation {

    private static final String ISOFOX = "isofox";

    private static final String ISOFOX_LOCATION = String.format("%s/%s", RNA_RESOURCES, ISOFOX);

    private static final String ISOFOX_JAR = "isofox.jar";
    private static final String RNA_BAM_FILE_ID = ".sorted.dups.bam";
    private static final String RNA_BAM_INDEX_FILE_ID = ".sorted.dups.bam.bai";

    private static final int COL_SAMPLE_ID = 0;
    private static final int COL_READ_LENGTH = 1;
    private static final int COL_GENE_IDS = 2;
    private static final int COL_SLICE_REGION = 3;

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
        final String geneIds = batchItems[COL_GENE_IDS];
        final String sliceRegion = batchItems[COL_SLICE_REGION];
        final RefGenomeVersion refGenomeVersion = V37;

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

        // slice the BAM for the required genes
        final String sambamba = "sambamba/0.6.8/sambamba";

        final String slicedBam = String.format("%s.spec_genes.bam", sampleId);

        startupScript.addCommand(() -> format("%s/%s slice %s/%s \"%s\" -o %s/%s",
                VmDirectories.TOOLS, sambamba, VmDirectories.INPUT, bamFile, sliceRegion, VmDirectories.OUTPUT, slicedBam));

        // commands.addCommand(() -> format("ls -l %s", VmDirectories.OUTPUT));

        final String slicedSortedBam = String.format("%s.spec_genes.sorted.bam", sampleId);

        // samtools sort -@ 8 -m 2G -T tmp -O bam Aligned.out.bam -o Aligned.sorted.bam
        final String[] sortArgs = {
                "sort", "-@", "8", "-m", "2G", "-T", "tmp",
                "-O", "bam", String.format("%s/%s", VmDirectories.OUTPUT, slicedBam),
                "-o", String.format("%s/%s", VmDirectories.OUTPUT, slicedSortedBam)};

        startupScript.addCommand(new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, sortArgs));

        // create an index
        startupScript.addCommand(() -> format("%s/%s index %s/%s", VmDirectories.TOOLS, sambamba, VmDirectories.OUTPUT, slicedSortedBam));

        // copy down the executable
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
                ISOFOX_LOCATION, ISOFOX_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        // run Isofox
        StringJoiner isofoxArgs = new StringJoiner(" ");
        isofoxArgs.add(String.format("-sample %s", sampleId));
        isofoxArgs.add(String.format("-functions %s", FUNC_TRANSCRIPT_COUNTS));

        isofoxArgs.add(String.format("-output_dir %s/", VmDirectories.OUTPUT));
        isofoxArgs.add(String.format("-bam_file %s/%s", VmDirectories.INPUT, bamFile));

        isofoxArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        isofoxArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));

        isofoxArgs.add(String.format("-write_exon_data"));
        isofoxArgs.add(String.format("-write_read_data"));
        isofoxArgs.add(String.format("-restricted_gene_ids %s", geneIds));

        startupScript.addCommand(() -> format("java -jar %s/%s %s", VmDirectories.TOOLS, ISOFOX_JAR, isofoxArgs.toString()));

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "isofox"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-isofox").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .workingDiskSpaceGb(MAX_EXPECTED_BAM_SIZE_GB)
                .build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("RnaMiscExpression", "Use Isofox for specific expression analysis",
                OperationDescriptor.InputType.FLAT);
    }

}
