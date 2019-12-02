package com.hartwig.batch.operations;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.InputFileDescriptor;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SagePon implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputFileDescriptor descriptor, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {


        // TODO: FILL THESE OUT
        String referenceSampleName = "";
        String referenceBamPath = "";
        String tumorSampleName = "";
        String tumorBamPath = "";
        String output = referenceSampleName + ".sage.vcf.gz";

        final String panel = String.format("%s/%s", VmDirectories.RESOURCES, "ActionableCodingPanel.hg19.bed.gz");
        final String hotspots = String.format("%s/%s", VmDirectories.RESOURCES, "KnownHotspots.hg19.vcf.gz");
        final String refGenome = "/opt/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta";

        final BashCommand sageCommand = new JavaClassCommand("sage",
                "pilot",
                "sage.jar",
                "com.hartwig.hmftools.sage.SageApplication",
                "32GB",
                "-reference",
                referenceSampleName,
                "-reference_bam",
                referenceBamPath,
                "-tumor",
                tumorSampleName,
                "-tumor_bam",
                tumorBamPath,
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

        // TODO: Upload output somewhere

        return null;
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("SagePon", "Generate sage output for PON creation");
    }
}
