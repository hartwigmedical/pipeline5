package com.hartwig.pipeline.upload;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class SBPS3StreamSupplierTest {

    @Test
    public void displayFileContents() throws Exception {
        InputStream stream = SBPS3StreamSupplier.newInstance("test").apply(new File("CPCT12345678R_HJJLGCCXX_S1_L001_R1_001.fastq.gz"));
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }
}