package com.hartwig.pipeline;

import java.util.List;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.ApiFileOperation;
import com.hartwig.pipeline.report.ReportComponent;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public interface StageOutput {

    String name();

    PipelineStatus status();

    List<ReportComponent> reportComponents();

    List<ApiFileOperation> furtherOperations();

    List<GoogleStorageLocation> failedLogLocations();
}