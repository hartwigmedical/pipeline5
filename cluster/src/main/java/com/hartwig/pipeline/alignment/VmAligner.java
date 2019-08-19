package com.hartwig.pipeline.alignment;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.cloud.storage.Storage;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.sample.SampleData;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.BatchInputDownload;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.trace.StageTrace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VmAligner implements Aligner {
    private final Logger LOGGER = LoggerFactory.getLogger(VmAligner.class);
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final SampleSource sampleSource;
    private final SampleUpload sampleUpload;
    private final ResultsDirectory resultsDirectory;
    private final AlignmentOutputStorage alignmentOutputStorage;

    VmAligner(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final SampleSource sampleSource,
            final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory, final AlignmentOutputStorage alignmentOutputStorage) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.sampleSource = sampleSource;
        this.sampleUpload = sampleUpload;
        this.resultsDirectory = resultsDirectory;
        this.alignmentOutputStorage = alignmentOutputStorage;
    }

    @Override
    public AlignmentOutput run(final SingleSampleRunMetadata metadata) throws Exception {
        if (!arguments.runAligner()) {
            return alignmentOutputStorage.get(metadata)
                    .orElseThrow(() -> new IllegalArgumentException(format(
                            "Unable to find output for sample [%s]. Please run the aligner first by setting -run_aligner to true",
                            arguments.sampleId())));
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);

        ResourceDownload referenceGenomeDownload =
                ResourceDownload.from(runtimeBucket, new Resource(storage, arguments.resourceBucket(), ResourceNames.REFERENCE_GENOME));
        String referenceGenomePath = referenceGenomeDownload.find("fa", "fasta");

        SampleData sampleData = sampleSource.sample(metadata, arguments);
        Sample sample = sampleData.sample();
        if (arguments.upload()) {
            sampleUpload.run(sample, runtimeBucket);
        }

        unzipFastqFiles(runtimeBucket, sampleData);

        ExecutorService executorService = Executors.newFixedThreadPool(sampleData.sample().lanes().size());
        List<Future<?>> futures = new ArrayList<>();

        for (Lane lane: sampleData.sample().lanes()) {
            Pattern pattern = Pattern.compile("^.*_S\\d+_L(\\d+)_R\\d+_\\d+\\.fastq\\.gz$");
            Matcher matcher = pattern.matcher(lane.firstOfPairPath());
            if (!matcher.matches()) {
                throw new RuntimeException(format("Cannot find lane name in FASTQ filename [%s]", lane.firstOfPairPath()));
            }
            String laneName = matcher.group(1);

            BashStartupScript bash = BashStartupScript.of(runtimeBucket.name(), laneName);

            String first = format("%s/%s", VmDirectories.INPUT, new File(lane.firstOfPairPath()).getName().replaceAll("\\.gz$", ""));
            String second = format("%s/%s", VmDirectories.INPUT, new File(lane.secondOfPairPath()).getName()).replaceAll("\\.gz$", "");
            BwaCommand bwa = new BwaCommand("mem", "-Y", "-t", Bash.allCpus(), referenceGenomePath, first, second);
            String sambamba = "/data/tools/sambamba/0.6.8/sambamba";

            BashCommand toBam = () -> sambamba + " view -f bam -S -l0 /dev/stdin";
            BashCommand sortBam = () -> sambamba + " sort -o " + VmDirectories.outputFile(laneName + "-aligned-sorted.bam") + " /dev/stdin";
            BashCommand chmodSambamba = () -> "chmod +x " + sambamba;

            bash.addCommand(referenceGenomeDownload)
                    .addCommand(chmodSambamba)
                    .addCommand(new InputDownload(GoogleStorageLocation.of(runtimeBucket.name(), format("samples/%s/%s",
                            sampleData.sample().name(), new File(first).getName()))))
                    .addCommand(new InputDownload(GoogleStorageLocation.of(runtimeBucket.name(), format("samples/%s/%s",
                            sampleData.sample().name(), new File(second).getName()))))
                    .addCommand(new PipeCommands(bwa, toBam, sortBam))
                    //.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
                .addCommand(() -> format("gsutil -qm cp %s/*.bam %s/*.bai gs://%s/results/", VmDirectories.OUTPUT, VmDirectories.OUTPUT, runtimeBucket.name()));

            computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.alignment(laneName, bash, resultsDirectory));
        }

//        PipelineStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.alignment(bash, resultsDirectory));
        return null;
    }

    private void unzipFastqFiles(final RuntimeBucket runtimeBucket, final SampleData sampleData) {
        Map<String, List<String>> lanesAndFastqs = new HashMap<>();
        ArrayList<InputDownload> inputDownloads = new ArrayList<>();
        List<String> fastqs = new ArrayList<>();
        for (Lane lane: sampleData.sample().lanes()) {
            Pattern pattern = Pattern.compile("^.*_S\\d+_L(\\d+)_R\\d+_\\d+\\.fastq\\.gz$");
            Matcher matcher = pattern.matcher(lane.firstOfPairPath());
            if (!matcher.matches()) {
                throw new RuntimeException(format("Cannot find lane name in FASTQ filename [%s]", lane.firstOfPairPath()));
            }

            String leftGz = format("%s/%s", VmDirectories.INPUT, new File(lane.firstOfPairPath()).getName());
            String rightGz = format("%s/%s", VmDirectories.INPUT, new File(lane.secondOfPairPath()).getName());

            lanesAndFastqs.put(matcher.group(1), asList(leftGz, rightGz));
            fastqs.add(leftGz);
            fastqs.add(rightGz);
            inputDownloads.add(new InputDownload(GoogleStorageLocation.of(runtimeBucket.name(), format("samples/%s/%s",
                    sampleData.sample().name(), new File(lane.firstOfPairPath()).getName()))));
            inputDownloads.add(new InputDownload(GoogleStorageLocation.of(runtimeBucket.name(), format("samples/%s/%s",
                    sampleData.sample().name(), new File(lane.secondOfPairPath()).getName()))));
        }
        if (fastqs.size() == 0 || fastqs.size() % 2 != 0) {
            throw new RuntimeException(format("Expected a non-zero, even number of FASTQ files but got %d!", fastqs.size()));
        }

        BashStartupScript startupScript = BashStartupScript.of(runtimeBucket.name(), "gunzip")
                .addCommand(new BatchInputDownload(inputDownloads.toArray(new InputDownload[]{})))
                .addCommand(() -> "parallel gunzip -kd ::: " + fastqs.stream().collect(Collectors.joining(" ")))
                .addCommand(new MvCommand(VmDirectories.INPUT + "/*.fastq", VmDirectories.OUTPUT))
                //.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), format("samples/%s", sampleData.sample().name()))));
                .addCommand(() -> format("gsutil -qm cp %s/*.fastq gs://%s/samples/%s", VmDirectories.OUTPUT, runtimeBucket.name(),
                        sampleData.sample().name()));

        LOGGER.info("Starting up {}-core machine for FASTQ decompression", fastqs.size());
        PipelineStatus status = computeEngine.submit(runtimeBucket,
                VirtualMachineJobDefinition.fastqGunzip(fastqs.size(), startupScript, resultsDirectory));
        if (status.equals(PipelineStatus.FAILED)) {
            throw new RuntimeException("Failed to decompress FASTQ files; see logs for details");
        }
    }
}
