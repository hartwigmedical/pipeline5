package com.hartwig.batch.operations;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SagePon implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputFileDescriptor descriptor, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        String[] values = descriptor.remoteFilename().split(",");

        String bucket = values[0];
        String set = values[1];
        String tumorSampleName = values[2];
        String referenceSampleName = tumorSampleName.substring(0, 12) + "R";
        String tumorBamFile = tumorSampleName + "_dedup.realigned.bam";
        String referenceBamFile = referenceSampleName + "_dedup.realigned.bam";

        String gcTumorBamFile = String.format("gs://%s/%s/%s/mapping/%s", bucket, set, tumorSampleName, tumorBamFile);
        String gcReferenceBamFile = String.format("gs://%s/%s/%s/mapping/%s", bucket, set, referenceSampleName, referenceBamFile);

        String localTumorBamFile = String.format("%s/%s", VmDirectories.INPUT, tumorBamFile);
        String localReferenceBamFile = String.format("%s/%s", VmDirectories.INPUT, referenceBamFile);

        final String output = String.format("%s/%s.sage.vcf.gz", VmDirectories.OUTPUT, tumorSampleName);
        final String panel = "/opt/resources/sage/ActionableCodingPanel.hg19.bed.gz";
        final String hotspots = "/opt/resources/sage/KnownHotspots.hg19.vcf.gz";
        final String refGenome = "/opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta";

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
                refGenome,
                "-out",
                output,
                "-threads",
                Bash.allCpus());

        // Download required resources
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/sage.jar", "/opt/tools/sage/pilot/sage.jar"));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/ActionableCodingPanel.hg19.bed.gz", panel));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/KnownHotspots.hg19.vcf.gz", hotspots));

        // Download bams
        startupScript.addCommand(() -> "touch /data/input/files.list");
        startupScript.addCommand(() -> format("echo %s | tee -a  /data/input/files.list", gcTumorBamFile));
        startupScript.addCommand(() -> format("echo %s.bai | tee -a /data/input/files.list", gcTumorBamFile));
        startupScript.addCommand(() -> format("echo %s | tee -a /data/input/files.list", gcReferenceBamFile));
        startupScript.addCommand(() -> format("echo %s.bai | tee -a /data/input/files.list", gcReferenceBamFile));
        startupScript.addCommand(() -> format("cat /data/input/files.list | gsutil -m -u hmf-crunch cp -I %s", "/data/input/"));

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

        return VirtualMachineJobDefinition.sage(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("SagePon", "Generate sage output for PON creation");
    }
}
