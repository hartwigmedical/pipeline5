package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.util.Optional;
import java.util.StringJoiner;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class TeloBatch implements BatchOperation
{
    // tools and reference files
    private static final String COMMON_RESOURCES = "hmf-crunch-resources";
    private static final String TELO_DIR = "telo";
    private static final String TELO_JAR = "telo.jar";
    private static final String MAX_HEAP = "16G";

    private static final String TELO_VERSION = "1.0";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags)
    {
        final String sampleId = inputs.get("sampleId").inputValue();
        Optional<String> specificChromosome = Optional.empty();

        try
        {
            specificChromosome = Optional.of(inputs.get("specificChromosome").inputValue());
        }
        catch (IllegalArgumentException ignored)
        {
        }

        final InputFileDescriptor runData = inputs.get();

        final RemoteLocationsApi locationsApi = new RemoteLocationsApi(runData.billedProject(), sampleId);

        // download the telo.jar
        //InputDownload teloJarDownload = new InputDownload(GoogleStorageLocation.of(teloToolsBucket, teloToolsPath + "/telo.jar"), VmDirectories.TOOLS);
        // InputDownload teloJarDownload = downloadExperimentalVersion();
        // commands.addCommand(teloJarDownload);

        commands.addCommand(downloadExperimentalVersion());

                /*() -> format("gsutil -u hmf-crunch cp gs://%s/%s/%s %s",
                COMMON_RESOURCES, TELO_DIR, TELO_JAR, VmDirectories.TOOLS));*/

        InputDownload tumorBamDownload = InputDownload.turbo(locationsApi.getTumorAlignment());
        InputDownload tumorBamIndexDownload = InputDownload.turbo(locationsApi.getTumorAlignmentIndex());
        InputDownload referenceBamDownload = InputDownload.turbo(locationsApi.getReferenceAlignment());
        InputDownload referenceBamIndexDownload = InputDownload.turbo(locationsApi.getReferenceAlignmentIndex());

        // ref genome
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        // download the tumour and reference bam / index files
        commands.addCommand(tumorBamDownload);
        commands.addCommand(tumorBamIndexDownload);
        commands.addCommand(referenceBamDownload);
        commands.addCommand(referenceBamIndexDownload);

        commands.addCommand(makeTeloRunCommand(sampleId, "somatic", tumorBamDownload.getLocalTargetPath(), resourceFiles.refGenomeFile(), specificChromosome));
        commands.addCommand(makeTeloRunCommand(sampleId, "germline", referenceBamDownload.getLocalTargetPath(), resourceFiles.refGenomeFile(), specificChromosome));

        //JavaJarCommand jarCommand = new JavaJarCommand("telo", TELO_VERSION, "telo.jar", "16G", teloArgs);
        // commands.addCommand(jarCommand);

        // Store output
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), sampleId), executionFlags));

        // and copy the run log files to a single directory for convenience
        // String commonLogDir = String.format("gs://%s/%s/logs/", TELO_BUCKET, TELO_FOLDER);
        // commands.addCommand(() -> format("gsutil -m cp /data/output/*.log %s", commonLogDir));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("telo")
                .startupCommand(commands)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .workingDiskSpaceGb(500)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(16, 16))
                .build();
    }

    @Override
    public OperationDescriptor descriptor()
    {
        return OperationDescriptor.of("TeloBatch", "Generate telo output", OperationDescriptor.InputType.JSON);
    }

    private InputDownload downloadExperimentalVersion()
    {
        return new InputDownload(GoogleStorageLocation.of(COMMON_RESOURCES, TELO_DIR + "/" + TELO_JAR),
                VmDirectories.TOOLS);
    }

    private BashCommand makeTeloRunCommand(String sampleId, String sampleType, String bamLocalPath, String refGenomeLocalPath,
            Optional<String> specificChromosome)
    {
        StringJoiner teloArgs = new StringJoiner(" ");
        teloArgs.add("-sample_id");
        teloArgs.add(sampleId);
        teloArgs.add("-sample_type");
        teloArgs.add(sampleType);
        teloArgs.add("-bam_file");
        teloArgs.add(bamLocalPath);
        teloArgs.add("-ref_genome");
        teloArgs.add(refGenomeLocalPath);
        teloArgs.add("-output_dir");
        teloArgs.add(VmDirectories.OUTPUT);
        teloArgs.add("-threads");
        teloArgs.add(Bash.allCpus());

        if(specificChromosome.isPresent())
        {
            teloArgs.add("-specific_chr");
            teloArgs.add(specificChromosome.get());
        }

        return () -> format("java -Xmx%s -jar %s/%s %s",
                MAX_HEAP, VmDirectories.TOOLS, TELO_JAR, teloArgs);
    }
}
