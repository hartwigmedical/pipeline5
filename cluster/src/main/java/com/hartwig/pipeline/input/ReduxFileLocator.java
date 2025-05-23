package com.hartwig.pipeline.input;

import java.net.URI;

import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.gcp.StorageUtil;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.reference.api.DataType;
import com.hartwig.pipeline.reference.api.Pipeline;
import com.hartwig.pipeline.reference.api.PipelineFilePath;
import com.hartwig.pipeline.reference.api.PipelineFiles;
import com.hartwig.pipeline.reference.api.PipelineOutputStructure;
import com.hartwig.pipeline.reference.api.PipelineOutputTemporaryLocation;
import com.hartwig.pipeline.reference.api.PipelineRun;
import com.hartwig.pipeline.reference.api.SampleType;
import com.hartwig.pipeline.reference.api.Tool;

import org.apache.commons.lang3.StringUtils;

public class ReduxFileLocator {

    private final PipelineInput input;
    private final StorageUtil storageUtil;
    private final String project;
    private final PipelineOutputStructure inputBamDirectoryStructure;

    public ReduxFileLocator(final PipelineInput input, final Storage storage, final String project,
            final PipelineOutputStructure inputBamDirectoryStructure) {
        this.input = input;
        this.storageUtil = new StorageUtil(storage);
        this.project = project;
        this.inputBamDirectoryStructure = inputBamDirectoryStructure;
    }

    public GoogleStorageLocation locateJitterParamsFile(SingleSampleRunMetadata metadata) {
        return locateFile(metadata, com.hartwig.pipeline.reference.api.DataType.REDUX_JITTER_PARAMS);
    }

    public GoogleStorageLocation locateMsTableFile(SingleSampleRunMetadata metadata) {
        return locateFile(metadata, com.hartwig.pipeline.reference.api.DataType.REDUX_MS_TABLE);
    }

    private GoogleStorageLocation locateFile(SingleSampleRunMetadata metadata,
            com.hartwig.pipeline.reference.api.DataType dataType) {
        SampleInput sample = Inputs.sampleFor(input, metadata);
        var bamLocation = sample.bam().get();
        // Because the tumor and reference are not nullable, but they are not necessary for location the file, we use the placeholder ___
        var tumor = metadata.type().equals(SingleSampleRunMetadata.SampleType.TUMOR) ? metadata.sampleName() : "___";
        var reference = metadata.type().equals(SingleSampleRunMetadata.SampleType.REFERENCE) ? metadata.sampleName() : "___";
        var pipelineRun = new PipelineRun(Pipeline.DNA_6_0, tumor, reference);
        var sampleType = metadata.type().equals(SingleSampleRunMetadata.SampleType.TUMOR) ? SampleType.TUMOR : SampleType.REFERENCE;

        var bamTool = bamLocation.endsWith(".cram") ? Tool.CRAM : Tool.ALIGNER;
        var relativeBamPath = PipelineFiles.get(pipelineRun,
                        PipelineFiles.sampleTypeIs(sampleType),
                        PipelineFiles.dataTypeIsAnyOf(DataType.ALIGNED_READS),
                        PipelineFiles.toolIsAnyOf(bamTool))
                .stream()
                .findFirst()
                .map(it -> it.getPathOrNull(inputBamDirectoryStructure))
                .map(PipelineFilePath::toString)
                .orElseThrow();
        var rootDirectory = URI.create(bamLocation).resolve("../".repeat(StringUtils.countMatches(relativeBamPath, "/")));
        var pipelineOutputLocation = new PipelineOutputTemporaryLocation(rootDirectory, inputBamDirectoryStructure);

        var reduxFile =
                PipelineFiles.get(pipelineRun, PipelineFiles.sampleTypeIs(sampleType), PipelineFiles.dataTypeIsAnyOf(dataType))
                        .stream()
                        .findFirst()
                        .map(it -> it.getUriOrNull(pipelineOutputLocation))
                        .map(URI::toString)
                        .orElseThrow();
        if (!storageUtil.exists(reduxFile)) {
            // If the file is not found, it either means the user made a mistake, or the file really does not exist.
            // In the first case, we let the user know by crashing pipeline5.
            // In the second case, the user should redo marking duplicates to generate the file.
            throw new IllegalStateException(("Duplicate marking output file not found. Expected at location: '%s'. "
                                             + "If this is intentional, consider enabling the '--redo_duplicate_marking' flag.").formatted(
                    reduxFile));
        }
        return GoogleStorageLocation.from(reduxFile, project);
    }
}
