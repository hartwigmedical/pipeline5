package com.hartwig.patient;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface Sample extends FileSystemEntity, Named {

    List<Lane> lanes();

    String barcode();

    static ImmutableSample.Builder builder(final String directory, final String name) {
        return ImmutableSample.builder().directory(directory).name(name).barcode("none");
    }

    static ImmutableSample.Builder builder(final String directory, final String name, final String barcode) {
        return ImmutableSample.builder().directory(directory).name(name).barcode(barcode);
    }
}
