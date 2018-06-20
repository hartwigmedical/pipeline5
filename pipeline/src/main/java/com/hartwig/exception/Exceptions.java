package com.hartwig.exception;

public class Exceptions {

    public static RuntimeException noLanesInSample() {
        return new IllegalArgumentException("Pre processing requires a sample with at least one lane. "
                + "Check the sample directory and ensure lanes are present and in the convention of {sample}_L00{index}");
    }

    public static RuntimeException firstStageInPipelineDatasourceNotAvailable(Class<?> stageClass) {
        return new IllegalStateException(String.format("%s stage is the first stage of the pipeline and can only retrieve its data from "
                + "the ADAM Context. This is a programmatic error and should never occur in production.", stageClass.getSimpleName()));
    }
}
