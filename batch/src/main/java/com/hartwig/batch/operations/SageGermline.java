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
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SageGermline implements BatchOperation {
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

        final String output = String.format("%s/%s.sage.germline.vcf.gz", VmDirectories.OUTPUT, tumorSampleName);
        final String panelBed = "/opt/resources/sage/ActionableCodingPanel.hg19.bed.gz";
        final String hotspots = "/opt/resources/sage/KnownHotspots.hg19.vcf.gz";
        final String highConfidenceBed = "/opt/resources/sage/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz";

        final BashCommand sageCommand = new JavaClassCommand("sage",
                "pilot",
                "sage.jar",
                "com.hartwig.hmftools.sage.SageApplication",
                "100G",
                "-reference",
                referenceSampleName,
                "-reference_bam",
                localReferenceBamFile,
                "-tumor",
                tumorSampleName,
                "-tumor_bam",
                localTumorBamFile,
                "-germline -hard_filter -hard_min_tumor_qual 0 -hard_min_tumor_raw_alt_support 3 -hard_min_tumor_raw_base_quality 30",
                "-panel_bed",
                panelBed,
                "-high_confidence_bed",
                highConfidenceBed,
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
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/ActionableCodingPanel.hg19.bed.gz", panelBed));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/KnownHotspots.hg19.vcf.gz", hotspots));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz", highConfidenceBed));

        // Download bams (in parallel)
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

        return VirtualMachineJobDefinition.builder().name("sage").startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(48, 128))
                .namespacedResults(ResultsDirectory.defaultDirectory()).build();
    }

    String getInput(List<InputFileDescriptor> inputs, String key) {
        return inputs.stream().filter(input -> input.name().equals(key)).collect(Collectors.toList()).get(0).remoteFilename();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SageGermline", "Generate sage germline output for PON creation",
                OperationDescriptor.InputType.JSON);
    }
}
