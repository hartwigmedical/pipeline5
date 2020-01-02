package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SagePon implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
                                               final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        String bucket = inputs.get("bucket").remoteFilename();
        String set = inputs.get("set").remoteFilename();
        String tumorSampleName = inputs.get("tumorSample").remoteFilename();
        String referenceSampleName = inputs.get("referenceSample").remoteFilename();

        String tumorBamFile = tumorSampleName + "_dedup.realigned.bam";
        String referenceBamFile = referenceSampleName + "_dedup.realigned.bam";

        String gcTumorBamFile = String.format("gs://%s/%s/%s/mapping/%s", bucket, set, tumorSampleName, tumorBamFile);
        String gcReferenceBamFile = String.format("gs://%s/%s/%s/mapping/%s", bucket, set, referenceSampleName, referenceBamFile);

        String localTumorBamFile = String.format("%s/%s", VmDirectories.INPUT, tumorBamFile);
        String localReferenceBamFile = String.format("%s/%s", VmDirectories.INPUT, referenceBamFile);

        final String output = String.format("%s/%s.sage.vcf.gz", VmDirectories.OUTPUT, tumorSampleName);
        final String panel = "/opt/resources/sage/ActionableCodingPanel.hg19.bed.gz";
        final String hotspots = "/opt/resources/sage/KnownHotspots.hg19.vcf.gz";

        final BashCommand sageCommand = new JavaClassCommand("sage",
                "pilot",
                "sage.jar",
                "com.hartwig.hmftools.sage.SageApplication",
                "32G",
                "-reference",
                referenceSampleName,
                "-reference_bam",
                localReferenceBamFile,
                "-tumor",
                tumorSampleName,
                "-tumor_bam",
                localTumorBamFile,
                "-panel_only",
                "-panel",
                panel,
                "-hotspots",
                hotspots,
                "-ref_genome",
                Resource.REFERENCE_GENOME_FASTA,
                "-out",
                output,
                "-threads",
                Bash.allCpus());

        // Download required resources
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/sage.jar", "/opt/tools/sage/pilot/sage.jar"));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/ActionableCodingPanel.hg19.bed.gz", panel));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/KnownHotspots.hg19.vcf.gz", hotspots));

        // Download bams
        startupScript.addCommand(() -> "touch /data/inputs/files.list");
        startupScript.addCommand(() -> format("echo %s | tee -a  /data/inputs/files.list", gcTumorBamFile));
        startupScript.addCommand(() -> format("echo %s.bai | tee -a /data/inputs/files.list", gcTumorBamFile));
        startupScript.addCommand(() -> format("echo %s | tee -a /data/inputs/files.list", gcReferenceBamFile));
        startupScript.addCommand(() -> format("echo %s.bai | tee -a /data/inputs/files.list", gcReferenceBamFile));
        startupScript.addCommand(() -> format("cat /data/inputs/files.list | gsutil -m -u hmf-crunch cp -I %s", "/data/inputs/"));

        // Prevent errors about index being older than bam
        startupScript.addCommand(() -> format("touch %s.bai", localTumorBamFile));
        startupScript.addCommand(() -> format("touch %s.bai", localReferenceBamFile));


        //        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", gcTumorBamFile, localTumorBamFile + ".bai"));
        //        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", gcTumorBamFile + ".bai", localTumorBamFile + ".bai"));
        //        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", gcReferenceBamFile, localReferenceBamFile));
        //        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", gcReferenceBamFile + ".bai", localReferenceBamFile + ".bai"));

        // Run Sage
        startupScript.addCommand(sageCommand);

        // Store output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return VirtualMachineJobDefinition.builder().name("sage").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).build();
    }

    String getInput(List<InputFileDescriptor> inputs, String key) {
        return inputs.stream().filter(input -> input.name().equals(key)).collect(Collectors.toList()).get(0).remoteFilename();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SagePon", "Generate sage output for PON creation",
                OperationDescriptor.InputType.JSON);
    }
}
