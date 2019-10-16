package com.hartwig.pipeline.execution.vm;

import org.apache.commons.lang3.RandomStringUtils;

class DataFixture {
    static String randomStr() {
        return RandomStringUtils.randomAlphabetic(16);
    }
}
