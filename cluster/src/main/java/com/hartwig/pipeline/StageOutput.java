package com.hartwig.pipeline;

import java.util.List;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.OutputComponent;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public interface StageOutput {

    String name();

    PipelineStatus status();

    List<OutputComponent> reportComponents();

    List<AddDatatype> datatypes();

    List<GoogleStorageLocation> failedLogLocations();
}