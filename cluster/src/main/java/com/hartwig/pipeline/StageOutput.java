package com.hartwig.pipeline;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.OutputComponent;

import java.util.List;

public interface StageOutput {

    String name();

    PipelineStatus status();

    List<OutputComponent> reportComponents();

    List<AddDatatype> datatypes();

    List<GoogleStorageLocation> failedLogLocations();
}