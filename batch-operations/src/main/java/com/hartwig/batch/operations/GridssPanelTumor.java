package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.PANEL_BAM_BUCKET;
import static com.hartwig.batch.operations.GridssBackport.index;
import static com.hartwig.batch.operations.GridssBackport.remoteUnfilteredVcfArchivePath;
import static com.hartwig.batch.operations.SageRerun.cramToBam;
import static com.hartwig.pipeline.calling.structural.gridss.stage.RepeatMasker.REPEAT_MASKER_TOOL;

import java.io.File;
import java.util.StringJoiner;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssCommand;
import com.hartwig.pipeline.calling.structural.gridss.stage.Driver;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssAnnotation;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class GridssPanelTumor implements BatchOperation {

    private static final String GRIDSS_TOOL_DIR = "gridss";
    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String sampleId = descriptor.inputValue();

        // download tumor BAM
        final String tumorBam = String.format("%s.non_umi_dedup.bam", sampleId);
        final String tumorBamIndex = String.format("%s.non_umi_dedup.bam.bai", sampleId);

        commands.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s* %s",
                PANEL_BAM_BUCKET, tumorBam, VmDirectories.INPUT));

        // Inputs
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V38);

        commands.addCommand(new ExportPathCommand(new BwaCommand()));
        commands.addCommand(new ExportPathCommand(new SamtoolsCommand()));

        // run Gridss variant calling
        final String gridssToolDir = String.format("%s/%s/%s", VmDirectories.TOOLS, GRIDSS_TOOL_DIR, Versions.GRIDSS);
        final String gridssJar = String.format("%s/gridss.jar", gridssToolDir);

        commands.addCommand(() -> format("chmod a+x %s", gridssJar));

        final String gridssOutputVcf = String.format("%s/%s.gridss.driver.vcf.gz", VmDirectories.OUTPUT, sampleId);

        final StringJoiner gridssArgs = new StringJoiner(" ");
        gridssArgs.add(String.format("--output %s", gridssOutputVcf));
        gridssArgs.add(String.format("--assembly %s/%s.gridss.assembly.vcf.gz", VmDirectories.OUTPUT, sampleId));
        gridssArgs.add(String.format("--workingdir %s/gridss_working", VmDirectories.OUTPUT));
        gridssArgs.add(String.format("--reference %s", resourceFiles.refGenomeFile()));
        gridssArgs.add(gridssJar);
        gridssArgs.add(String.format("--blacklist %s", resourceFiles.gridssBlacklistBed()));
        gridssArgs.add(String.format("--configuration %s", resourceFiles.gridssPropertiesFile()));
        gridssArgs.add(String.format("--labels %s", sampleId));
        gridssArgs.add(String.format("--threads %s", Bash.allCpus()));
        gridssArgs.add("--jvmheap 31G");
        gridssArgs.add("--externalaligner");

        gridssArgs.add(String.format("%s/%s", VmDirectories.INPUT, tumorBam));

        // nohup /data/tools/gridss/2.13.2/gridss
        // --output ./FR16648841.gridss.driver.vcf.gz
        // --assembly ./FR16648841.assembly.bam
        // --workingdir ./gridss_working
        // --reference /data/resources/bucket/reference_genome/38/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna
        // --jar /data/tools/gridss/2.13.2/gridss.jar
        // --blacklist /data/resources/public/gridss_repeatmasker_db/38/ENCFF001TDO.38.bed
        // --configuration /data/resources/public/gridss_config/gridss.properties
        // --labels FR16648841
        // --jvmheap 31G
        // --threads 4
        // --externalaligner
        // FR16648841.chr21_slice1.bam &

        commands.addCommand(() -> format("%s/gridss %s", gridssToolDir, gridssArgs.toString()));

        // VersionedToolCommand with bash:
        // /opt/tools/gridss/2.13.2/gridss_annotate_vcf_repeatmasker
        // --output /data/output/CPCT12345678T.gridss.repeatmasker.vcf.gz
        // --jar /opt/tools/gridss/2.13.2/gridss.jar
        // -w /data/output
        // --rm /opt/tools/repeatmasker/4.1.1/RepeatMasker
        // /data/output/CPCT12345678T.gridss.driver.vcf.gz

        // final String gridssToolDir = String.format("%s/%s/%s/", VmDirectories.TOOLS, GRIDSS_TOOL_DIR, Versions.GRIDSS);
        final String rmOutputVcf = String.format("%s/%s.gridss.repeatmasker.vcf.gz", VmDirectories.OUTPUT, sampleId);

        final StringJoiner rmArgs = new StringJoiner(" ");
        rmArgs.add(String.format("--output %s", rmOutputVcf));
        rmArgs.add(String.format("--jar %s", gridssJar));
        rmArgs.add(String.format("-w %s", VmDirectories.OUTPUT));
        rmArgs.add(String.format("--rm %s", REPEAT_MASKER_TOOL));
        rmArgs.add(gridssOutputVcf);

        commands.addCommand(() -> format("%s/gridss_annotate_vcf_repeatmasker %s", gridssToolDir, rmArgs.toString()));

        // AnnotateInsertedSequence with bash:
        // java -Xmx8G -Dsamjdk.create_index=true
        // -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true
        // -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304
        // -cp /opt/tools/gridss/2.13.2/gridss.jar gridss.AnnotateInsertedSequence
        // REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa
        // INPUT=/data/output/CPCT12345678T.gridss.repeatmasker.vcf.gz
        // OUTPUT=/data/output/CPCT12345678T.gridss.unfiltered.vcf.gz
        // ALIGNMENT=APPEND WORKER_THREADS=12

        final String finalOutputVcf = String.format("%s/%s.gridss.unfiltered.vcf.gz", VmDirectories.OUTPUT, sampleId);

        final StringJoiner vmArgs = new StringJoiner(" ");
        GridssCommand.JVM_ARGUMENTS.forEach(x -> vmArgs.add(x));

        final StringJoiner annInsSeqArgs = new StringJoiner(" ");
        annInsSeqArgs.add(String.format("REFERENCE_SEQUENCE=%s", resourceFiles.gridssVirusRefGenomeFile()));
        annInsSeqArgs.add(String.format("INPUT=%s", rmOutputVcf));
        annInsSeqArgs.add(String.format("OUTPUT=%s", finalOutputVcf));
        annInsSeqArgs.add(String.format("ALIGNMENT=APPEND WORKER_THREADS=%s", Bash.allCpus()));

        commands.addCommand(() -> format("java -Xmx8G -Dsamjdk.create_index=true %s -cp %s gridss.AnnotateInsertedSequence %s",
                vmArgs.toString(), gridssJar, annInsSeqArgs.toString()));

        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "gridss"), executionFlags));

        return VirtualMachineJobDefinition.structuralCalling(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("GridssPanelTumor", "Run Gridss on tumor panel", OperationDescriptor.InputType.FLAT);
    }

        /*
        final String set = inputs.get("set").inputValue();
        final String tumorSampleName = inputs.get("tumor_sample").inputValue();
        final String referenceSampleName = inputs.get("reference_sample").inputValue();
        final InputFileDescriptor remoteTumorFile = inputs.get("tumor_cram");
        final InputFileDescriptor remoteReferenceFile = inputs.get("ref_cram");

        final InputFileDescriptor runData = inputs.get();
        final RemoteLocationsApi locationsApi = new RemoteLocationsApi(runData.billedProject(), tumorSampleName);

        final InputFileDescriptor remoteTumorIndex = remoteTumorFile.index();
        final InputFileDescriptor remoteReferenceIndex = remoteReferenceFile.index();

        final String localTumorFile = localFilename(remoteTumorFile);
        final String localReferenceFile = localFilename(remoteReferenceFile);

        final String tumorBamPath = localTumorFile.replace("cram", "bam");
        final String refBamPath = localReferenceFile.replace("cram", "bam");

        String tumorBamPath = String.format("%s/%s", VmDirectories.INPUT, tumorBam);

        Driver driver = new Driver(resourceFiles,
                referenceSampleName,
                tumorSampleName,
                VmDirectories.outputFile(sampleId + ".assembly.bam"),
                refBamPath,
                tumorBamPath);

        GridssAnnotation viralAnnotation = new GridssAnnotation(resourceFiles, false);
        SubStageInputOutput unfilteredVcfOutput = driver.andThen(viralAnnotation).apply(SubStageInputOutput.empty(tumorSampleName));

        final OutputFile unfilteredVcf = unfilteredVcfOutput.outputFile();
        final OutputFile unfilteredVcfIndex = unfilteredVcf.index(".tbi");
        final GoogleStorageLocation unfilteredVcfRemoteLocation = remoteUnfilteredVcfArchivePath(set, tumorSampleName);
        final GoogleStorageLocation unfilteredVcfIndexRemoteLocation = index(unfilteredVcfRemoteLocation, ".tbi");

        commands.addCommand(() -> remoteTumorFile.toCommandForm(localTumorFile));
        commands.addCommand(() -> remoteTumorIndex.toCommandForm(localFilename(remoteTumorIndex)));
        commands.addCommand(() -> remoteReferenceFile.toCommandForm(localReferenceFile));
        commands.addCommand(() -> remoteReferenceIndex.toCommandForm(localFilename(remoteReferenceIndex)));
        if (!localTumorFile.equals(tumorBamPath)) {
            commands.addCommands(cramToBam(localTumorFile));
        }
        if (!localReferenceFile.equals(refBamPath)) {
            commands.addCommands(cramToBam(localReferenceFile));
        }

        commands.addCommands(unfilteredVcfOutput.bash());
        commands.addCommand(() -> unfilteredVcf.copyToRemoteLocation(unfilteredVcfRemoteLocation));
        commands.addCommand(() -> unfilteredVcfIndex.copyToRemoteLocation(unfilteredVcfIndexRemoteLocation));
        */

}
