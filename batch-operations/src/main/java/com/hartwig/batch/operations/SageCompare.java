package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.api.RemoteLocationsApi.CRAM_FILENAME;
import static com.hartwig.batch.api.RemoteLocationsApi.CRAM_FULL_PATH;
import static com.hartwig.batch.api.RemoteLocationsApi.getCramFileData;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.SageRerunOld.cramToBam;
import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;

import java.util.StringJoiner;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class SageCompare implements BatchOperation {

    private static final String SAGE_DIR = "sage";
    private static final String SAGE_JAR = "sage.jar";

    private static final String RUN_BOTH = "both";
    private static final String RUN_NEW = "new";
    private static final String RUN_OLD = "old";
    private static final String RUN_CRAM_VS_BAM = "cram_vs_bam";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String[] sampleData = descriptor.inputValue().split(",",-1);

        final String sampleId = sampleData[0];
        String runTypes = sampleData.length > 1 ? sampleData[1] : RUN_BOTH;

        boolean runBoth = runTypes.equalsIgnoreCase(RUN_BOTH);
        boolean cramVsBam = runTypes.equalsIgnoreCase(RUN_CRAM_VS_BAM);
        boolean runOld = runBoth || runTypes.equalsIgnoreCase(RUN_OLD);
        boolean runNew = runBoth || cramVsBam || runTypes.equalsIgnoreCase(RUN_NEW);

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, SAGE_DIR, SAGE_JAR, VmDirectories.TOOLS));

        final RemoteLocationsApi locations = new RemoteLocationsApi("hmf-crunch", sampleId);

        String[] tumorCramData = getCramFileData(locations.getTumorAlignment());
        String tumorCramFile = tumorCramData[CRAM_FILENAME];

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s* %s", tumorCramData[CRAM_FULL_PATH], VmDirectories.INPUT));

        String referenceId = locations.getReference();

        String[] refCramData = getCramFileData(locations.getReferenceAlignment());
        String refCramFile = refCramData[CRAM_FILENAME];

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s* %s", refCramData[CRAM_FULL_PATH], VmDirectories.INPUT));

        // download tumor CRAM
        String localTumorCram = String.format("%s/%s", VmDirectories.INPUT, tumorCramFile);
        String localRefCram = String.format("%s/%s", VmDirectories.INPUT, refCramFile);

        // and convert to BAM
        startupScript.addCommands(cramToBam(localTumorCram));
        startupScript.addCommands(cramToBam(localRefCram));

        String localTumorBam = localTumorCram.replace("cram", "bam");
        String localRefBam = localRefCram.replace("cram", "bam");

        if(runOld)
        {
            final String oldSageVcf = String.format("%s/%s.sage.somatic.vcf.gz", VmDirectories.OUTPUT, sampleId);

            // run old Sage
            final StringJoiner oldSageArgs = new StringJoiner(" ");
            oldSageArgs.add(String.format("-tumor %s", sampleId));
            oldSageArgs.add(String.format("-tumor_bam %s", localTumorBam));
            oldSageArgs.add(String.format("-reference %s", referenceId));
            oldSageArgs.add(String.format("-reference_bam %s", localRefBam));
            oldSageArgs.add(String.format("-hotspots %s", resourceFiles.sageSomaticHotspots()));
            oldSageArgs.add(String.format("-panel_bed %s", resourceFiles.sageSomaticCodingPanel()));
            oldSageArgs.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));

            oldSageArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
            oldSageArgs.add("-assembly hg19");
            oldSageArgs.add("-bqr_plot false");
            oldSageArgs.add(String.format("-out %s", oldSageVcf));
            oldSageArgs.add(String.format("-threads %s", Bash.allCpus()));
            // oldSageArgs.add("-chr 14");

            String oldSageJar = String.format("sage/%s/sage.jar", Versions.SAGE);

            startupScript.addCommand(() -> format("java -Xmx48G -jar %s/%s %s", VmDirectories.TOOLS, oldSageJar, oldSageArgs.toString()));
        }

        if(runNew)
        {
            final String newSageVcf = String.format("%s/%s.sage.somatic.vcf.gz", VmDirectories.OUTPUT, sampleId);

            final StringJoiner newSageArgs = new StringJoiner(" ");
            newSageArgs.add(String.format("-tumor %s", sampleId));
            newSageArgs.add(String.format("-tumor_bam %s", localTumorBam));
            newSageArgs.add(String.format("-reference %s", referenceId));
            newSageArgs.add(String.format("-reference_bam %s", localRefBam));
            newSageArgs.add(String.format("-hotspots %s", resourceFiles.sageSomaticHotspots()));
            newSageArgs.add(String.format("-panel_bed %s", resourceFiles.sageSomaticCodingPanel()));
            newSageArgs.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));
            newSageArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
            newSageArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
            newSageArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
            newSageArgs.add(String.format("-perf_warn_time 50"));
            newSageArgs.add(String.format("-log_debug"));
            newSageArgs.add(String.format("-out %s", newSageVcf));
            newSageArgs.add(String.format("-threads %s", Bash.allCpus()));

            startupScript.addCommand(() -> format("java -Xmx48G -jar %s/%s %s", VmDirectories.TOOLS, SAGE_JAR, newSageArgs.toString()));
        }

        if(cramVsBam)
        {
            final String newCramSageVcf = String.format("%s/%s.sage.somatic.cram.vcf.gz", VmDirectories.OUTPUT, sampleId);

            final StringJoiner newSageArgs = new StringJoiner(" ");
            newSageArgs.add(String.format("-tumor %s", sampleId));
            newSageArgs.add(String.format("-tumor_bam %s", localTumorCram));
            newSageArgs.add(String.format("-reference %s", referenceId));
            newSageArgs.add(String.format("-reference_bam %s", localRefCram));
            newSageArgs.add(String.format("-hotspots %s", resourceFiles.sageSomaticHotspots()));
            newSageArgs.add(String.format("-panel_bed %s", resourceFiles.sageSomaticCodingPanel()));
            newSageArgs.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));
            newSageArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
            newSageArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
            newSageArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
            newSageArgs.add(String.format("-perf_warn_time 50"));
            newSageArgs.add(String.format("-log_debug"));
            newSageArgs.add(String.format("-out %s", newCramSageVcf));
            newSageArgs.add(String.format("-threads %s", Bash.allCpus()));

            startupScript.addCommand(() -> format("java -Xmx48G -jar %s/%s %s", VmDirectories.TOOLS, SAGE_JAR, newSageArgs.toString()));
        }

        // upload output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("sage")
                .startupCommand(startupScript)
                .performanceProfile(custom(24, 64))
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SageCompare", "Sage new vs old", OperationDescriptor.InputType.FLAT);
    }
}
