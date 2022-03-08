package com.hartwig.batch.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.logging.Logger;

import com.google.common.collect.Maps;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class SampleLocationData
{
    public final String SampleId;
    public final String ReferenceId;
    public final String TumorBam;
    public final String ReferenceBam;
    public final String SageGermlineVcf;
    public final String SageSomaticVcf;
    public final String GridssVcf;
    public final String GripssVcf;
    public final String PurpleSomaticVcf;
    public final String PurpleGermlineVcf;
    public final String Purple;
    public final String Amber;
    public final String Cobalt;

    private static final Logger LOGGER = Logger.getLogger(SampleLocationData.class.getName());

    public static final String PURPLE_GENE_COPY_NUMBER = "purple.cnv.gene.tsv";
    public static final String PURPLE_QC = "purple.qc";
    public static final String PURPLE_PURITY = "purple.purity.tsv";

    public static final String OLD_BUCKET_PREFIX = "hmf-output";

    public SampleLocationData(
            final String sampleId, final String referenceId,
            final String tumorBam, final String referenceBam, final String sageGermlineVcf, final String sageSomaticVcf,
            final String gridssVcf, final String gripssVcf, final String purpleGermlineVcf, final String purpleSomaticVcf,
            final String purple, final String amber, final String cobalt)
    {
        SampleId = sampleId;
        ReferenceId = referenceId;
        TumorBam = tumorBam;
        ReferenceBam = referenceBam;
        SageGermlineVcf = sageGermlineVcf;
        SageSomaticVcf = sageSomaticVcf;
        GridssVcf = gridssVcf;
        GripssVcf = gripssVcf;
        PurpleSomaticVcf = purpleSomaticVcf;
        PurpleGermlineVcf = purpleGermlineVcf;
        Purple = purple;
        Amber = amber;
        Cobalt = cobalt;
    }

    public static SampleLocationData fromRemoteLocationsApi(final String sampleId, final RemoteLocationsApi locations)
    {
        // look for a bucket matching the sampleId to use by default
        String purpleSomaticVcf = bucketAndPath(locations.getSomaticVariantsPurple());
        String purpleGermlineVcf = bucketAndPath(locations.getGermlineVariantsPurple());
        String purpleDir = purpleSomaticVcf.substring(0, purpleSomaticVcf.lastIndexOf("/"));

        String tumorBam = bucketAndPath(locations.getTumorAlignment());
        String referenceBam = bucketAndPath(locations.getReferenceAlignment());

        return new SampleLocationData(
                sampleId, locations.getReference(), tumorBam, referenceBam,
                bucketAndPath(locations.getGermlineVariantsSage()), bucketAndPath(locations.getSomaticVariantsSage()),
                bucketAndPath(locations.getStructuralVariantsGridss()), bucketAndPath(locations.getStructuralVariantsGripss()),
                purpleSomaticVcf, purpleGermlineVcf, purpleDir,bucketAndPath(locations.getAmber()), bucketAndPath(locations.getCobalt()));
    }

    public static String bucketAndPath(final GoogleStorageLocation location)
    {
        String bucket = location.bucket();
        String path = location.path();

        if(!bucket.endsWith(File.separator))
            bucket += File.separator;

        return bucket + path;
    }

    private static String extractPath(final String sampleId, final GoogleStorageLocation location, final String defaultRunBucket)
    {
        if(location.bucket().equals(defaultRunBucket))
            return location.path();
        else
            return location.bucket() + "/" + location.path();

        /*
        String bucket = location.bucket();

        if(bucket.toUpperCase().contains(sampleId))
            return location.path();
        else
            return location.bucket() + "/" + location.path();
        */
    }

    private String getFileOnly(final String fileRef)
    {
        // strip version and directory data
        String[] fileData = fileRef.split("/");
        return fileData[fileData.length - 1];
    }

    public String localFileRef(final String fileRef)
    {
        String fileRefOnly = getFileOnly(fileRef);
        return String.format("%s/%s", VmDirectories.INPUT, fileRefOnly);
    }

    /*
    public String remotePath(final String fileRef)
    {
        return fileRef.contains(OLD_BUCKET_PREFIX) ? fileRef : String.format("%s/%s", RunBucket, fileRef);
    }
    */

    public String formDownloadRequest(final String fileRef, final boolean recursive)
    {
        String remotePath = fileRef; // remotePath(fileRef);

        return String.format("gsutil -m -u hmf-crunch %s gs://%s %s/",
                recursive ? "cp -r" : "cp", remotePath, VmDirectories.INPUT);
    }

    public String remotePurpleFile(final String fileSuffix)
    {
        return String.format("gs://%s/%s.%s" + Purple, SampleId, fileSuffix);
    }

    private static final String DELIM = ",";

    public static String csvHeader()
    {
        StringJoiner sj = new StringJoiner(DELIM);
        sj.add("SampleId");
        sj.add("ReferenceId");
        sj.add("TumorBam");
        sj.add("ReferenceBam");
        sj.add("SageSomaticVcf");
        sj.add("SageGermlineVcf");
        sj.add("GridssVcf");
        sj.add("GripssVcf");
        sj.add("PurpleSomaticVcf");
        sj.add("PurpleGermlineVcf");
        sj.add("Purple");
        sj.add("Amber");
        sj.add("Cobalt");
        return sj.toString();
    }

    public String csvData()
    {
        StringJoiner sj = new StringJoiner(DELIM);
        sj.add(SampleId);
        sj.add(ReferenceId);
        sj.add(TumorBam);
        sj.add(ReferenceBam);
        sj.add(SageSomaticVcf);
        sj.add(SageGermlineVcf);
        sj.add(GridssVcf);
        sj.add(GripssVcf);
        sj.add(PurpleSomaticVcf);
        sj.add(PurpleGermlineVcf);
        sj.add(Purple);
        sj.add(Amber);
        sj.add(Cobalt);
        return sj.toString();
    }

    public static Map<String,SampleLocationData> loadSampleLocations(final String filename, final List<String> restrictedSampleIds)
    {
        Map<String,SampleLocationData> sampleLocations = Maps.newHashMap();

        try
        {
            BufferedReader fileReader = new BufferedReader(new FileReader(filename));
            String header = fileReader.readLine();

            final String[] columns = header.split(DELIM,-1);
            final Map<String,Integer> fieldsIndexMap = Maps.newLinkedHashMap();

            for(int i = 0; i < columns.length; ++i)
            {
                fieldsIndexMap.put(columns[i], i);
            }

            int sampleIndex = fieldsIndexMap.get("SampleId");
            int refIndex = fieldsIndexMap.get("ReferenceId");

            int tumorBamIndex = fieldsIndexMap.get("TumorBam");
            int refBamIndex = fieldsIndexMap.get("ReferenceBam");
            int sageSomaticIndex = fieldsIndexMap.get("SageSomaticVcf");
            int sageGermlineIndex = fieldsIndexMap.get("SageGermlineVcf");
            int gridssIndex = fieldsIndexMap.get("GridssVcf");
            int gripssIndex = fieldsIndexMap.get("GripssVcf");
            int purpleSomaticVcfIndex = fieldsIndexMap.get("PurpleSomaticVcf");
            int purpleGermlineVcfIndex = fieldsIndexMap.get("PurpleGermlineVcf");
            int purpleIndex = fieldsIndexMap.get("Purple");
            int amberIndex = fieldsIndexMap.get("Amber");
            int cobaltIndex = fieldsIndexMap.get("Cobalt");

            String line = "";

            while((line = fileReader.readLine()) != null)
            {
                final String[] values = line.split(DELIM, -1);

                String sampleId = values[sampleIndex];

                if(!restrictedSampleIds.isEmpty() && !restrictedSampleIds.contains(sampleId))
                    continue;

                sampleLocations.put(sampleId, new SampleLocationData(
                        sampleId, values[refIndex], values[tumorBamIndex], values[refBamIndex],
                        values[sageGermlineIndex], values[sageSomaticIndex], values[gridssIndex],
                        values[gripssIndex], values[purpleSomaticVcfIndex], values[purpleGermlineVcfIndex],
                        values[purpleIndex], values[amberIndex], values[cobaltIndex]));
            }

            LOGGER.info(String.format("loaded %s sample locations from file(%s)", sampleLocations.size(), filename));
        }
        catch(IOException e)
        {
            LOGGER.severe("failed to read sample locations file: {}" + e.toString());
        }

        return sampleLocations;
    }
}
