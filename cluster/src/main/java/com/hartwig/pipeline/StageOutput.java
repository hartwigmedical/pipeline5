package com.hartwig.pipeline;

import java.util.List;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.OutputComponent;

public interface StageOutput {

    String name();

    PipelineStatus status();

    List<OutputComponent> reportComponents();

    List<AddDatatype> datatypes();

    List<GoogleStorageLocation> failedLogLocations();
}