package com.hartwig.pipeline.upload;

import java.util.Optional;

public interface SBPRestApi {

    Optional<String> getFastQ(String patientId);
}
