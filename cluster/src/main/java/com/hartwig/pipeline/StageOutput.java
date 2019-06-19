package com.hartwig.pipeline;

import java.util.List;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.report.ReportComponent;

public interface StageOutput {

    String name();

    PipelineStatus status();

    List<ReportComponent> reportComponents();
}
