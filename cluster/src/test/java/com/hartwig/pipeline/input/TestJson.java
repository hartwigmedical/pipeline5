package com.hartwig.pipeline.input;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.hartwig.pipeline.testsupport.Resources;

import org.jetbrains.annotations.NotNull;

public class TestJson {

    @NotNull
    public static String get(final String name) {
        try {
            return new String(Files.readAllBytes(Paths.get(Resources.testResource("pipeline-input/" + name + ".json"))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
