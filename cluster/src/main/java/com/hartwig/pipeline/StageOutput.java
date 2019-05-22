package com.hartwig.pipeline;

import java.util.List;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.report.ReportComponent;

public interface StageOutput {

    String name();

    JobStatus status();

    List<ReportComponent> reportComponents();
}
