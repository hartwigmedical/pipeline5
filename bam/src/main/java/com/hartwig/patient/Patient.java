package com.hartwig.patient;

import static java.lang.String.format;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface Patient extends FileSystemEntity, Named {

    @Value.Parameter
    @Override
    String directory();

    @Value.Parameter
    @Override
    String name();

    @Value.Parameter
    Sample reference();

    @Value.Parameter
    Optional<Sample> maybeTumor();

    default Sample tumor() {
        return maybeTumor().orElseThrow(() -> new IllegalStateException(format(
                "Patient [%s] has no tumor sample. Check the patient data provided",
                this)));
    }

    static Patient of(String directory, String name, Sample reference, Sample tumor) {
        return ImmutablePatient.of(directory, name, reference, Optional.of(tumor));
    }

    static Patient of(String directory, String name, Sample reference) {
        return ImmutablePatient.builder().directory(directory).name(name).reference(reference).build();
    }
}
