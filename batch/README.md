# Hartwig Medical Foundation - Batch5

This is a separate entry point used for running batch operations in the cloud. When you run the framework it will invoke a 
batch operation once for every path in your input file, maintain instances to keep up the desired parallelism, 
log progress and produce a report at the end of execution.

To use this framework:

1. Package a Java class implementing the `BatchOperation` interface) into a JAR file;
1. Write an input file with paths to your inputs;
1. Optionally create a Docker container from our Dockerhub image;
1. Run your operation in a live environment under Docker or your IDE using the JAR you made.

## Writing Batch Operations

A batch operation is a Java class that implements the `BatchOperation` interface. You can either check out the `pipeline5` project
and add an implementation in the `batch-operations` module or use Maven to import the framework as a dependency to your own project.

Regardless of your choice:

* Make sure each operation name returned in the `BatchDescriptor` is unique as the framework does not validate this.
* You must package your operation into a JAR file and copy it to the host of the Docker container you create later on. You can
  also choose to run using `BatchDispatcher` as your main class direct from your IDE but that approach is not covered here.

### Adding the Operation in the Project

Make your implementation of `BatchOperation` within the `batch-operations` module. When you are finished you may run `mvn clean package`
on the command line to create a JAR containing the operations, or export the module's code as a JAR from your IDE. This JAR file will
need to be copied to the machine hosting the Docker container.

### Adding the Project as a Maven Dependency

With this approach you don't pull the code, just reference the framework as a dependency in Maven. In your project's POM include at
least the following:

```
<project>
...
    <properties>
        <hartwig.version>5.x.y</hartwig.version>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
    </properties>

    <repositories>
        <repository>
            <id>hmf-maven-repository-release</id>
            <url>gs://hmf-maven-repository/release</url>
        </repository>
    </repositories>
    
    ...

    <dependencies>
        <dependency>
            <groupId>com.hartwig</groupId>
            <artifactId>batch</artifactId>
            <version>${hartwig.version}</version>
        </dependency>
    ...

    <build>
        <extensions>
            <extension>
                <groupId>com.gkatzioura.maven.cloud</groupId>
                <artifactId>google-storage-wagon</artifactId>
                <version>1.0</version>
            </extension>
...
```

Then you should have access to the `BatchOperation` interface and other classes your `BatchOperation` needs.

### Sample Operation

```
     1  package com.hartwig.batch.operations;
     2
     3  import com.hartwig.batch.BatchOperation;
     4  import com.hartwig.batch.input.InputBundle;
     5  import com.hartwig.batch.input.InputFileDescriptor;
     6  import com.hartwig.pipeline.ResultsDirectory;
     7  import com.hartwig.pipeline.calling.command.VersionedToolCommand;
     8  import com.hartwig.pipeline.execution.vm.Bash;
     9  import com.hartwig.pipeline.execution.vm.BashStartupScript;
    10  import com.hartwig.pipeline.execution.vm.OutputUpload;
    11  import com.hartwig.pipeline.execution.vm.RuntimeFiles;
    12  import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
    13  import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
    14  import com.hartwig.pipeline.execution.vm.VmDirectories;
    15  import com.hartwig.pipeline.resource.ResourceFiles;
    16  import com.hartwig.pipeline.storage.GoogleStorageLocation;
    17  import com.hartwig.pipeline.storage.RuntimeBucket;
    18  import com.hartwig.pipeline.tools.Versions;
    19
    20  import java.io.File;
    21
    22  public class SambambaCramaBam implements BatchOperation {
    23      @Override
    24      public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket bucket,
    25                                                 final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
    26          InputFileDescriptor input = inputs.get();
    27          String outputFile = VmDirectories.outputFile(new File(input.remoteFilename()).getName().replaceAll("\\.bam$", ".cram"));
    28          String localInput = String.format("%s/%s", VmDirectories.INPUT, new File(input.remoteFilename()).getName());
    29          startupScript.addCommand(() -> input.toCommandForm(localInput));
    30          startupScript.addCommand(new VersionedToolCommand("sambamba", "sambamba", Versions.SAMBAMBA,
    31                  "view", localInput, "-o", outputFile, "-t", Bash.allCpus(), "--format=cram",
    32                  "-T", Resource.REFERENCE_GENOME_FASTA));
    33          startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "cram"), executionFlags));
    34
    35          return VirtualMachineJobDefinition.builder().name("cram").startupCommand(startupScript)
    36                  .namespacedResults(ResultsDirectory.defaultDirectory())
    37                  .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 6))
    38                  .build();
    39      }
    40
    41      @Override
    42      public OperationDescriptor descriptor() {
    43          return OperationDescriptor.of("SambambaCramaBam", "Produce a CRAM file from each input BAM",
    44                  OperationDescriptor.InputType.FLAT);
    45      }
    46  }
```

Discussion:

* Line 1: For the moment make sure your operation lives in this package so the runtime locator finds it
* 3-18: Other HMF classes you're likely to need or interact with
* 22: Make sure you implement the `BatchOperation` interface
* 26: This operation uses a "flat" input file style (see line 44). A richer JSON version is also available; in that case you'd
  get elements from the input by key
* 29-33: Build up `bash` commands to be executed on the VM instance. Your commands need to be well-behaved and return 0 on
  success, non-zero on failure or the framework will not handle failure scenarios properly. Note that the final command here is an
  upload to the output bucket of the file that has been generated.
* 35-38: Describe performance characteristics of the VM.
* 41-44: Provide a description of the operation to allow the framework to find it and the user to invoke it.

## Writing an Input File

The format of the input file is defined by the operation. Look at the code for existing operations, but if you're writing your own
it is pretty much up to you what you put in here. There is a simple format that just runs one instance for every line in your file
and a more-powerful JSON version. The operation must return the type it will use in its descriptor.

## Configuring the GCP Environment

The batch needs to be run with credentials for a service account that can both read from the bucket that is referenced in the
batch descriptor and write to the output bucket. It also needs to be able to create and run VM instances in the chosen project.
Given bucket names are globally unique in GCP the buckets do not necessarily have to be in the same project. 

## Running your Operation

If you've written a custom operation it's easiest to start up the `Docker` container and execute your batch. If you have checked
the project out you will probably find your IDE is easiest.

### From the IDE

The entry point for the application is the `BatchDispatcher` class.

### Docker 

Create a Docker container from the `hartwigmedicalfoundation/batch5` image. Then make the JAR containing your class available for 
the `Docker` instance to pick up at runtime (the path on the Docker container is not configurable):

```
$ cp jar-containing-operation.jar /tmp/jarfiles
$ docker run -it -v /tmp/jarfiles:/usr/share/thirdpartyjars hartwigmedicalfoundation/batch5 MyOp -help ...
```

The container will periodically print out status information. You may wish to run the command above with GNU `screen` or equivalent
to maintain interactive control of the process as it runs. 

*NB* The `-it` in the command line is important as without it Docker will not be responsive to signals (such as Ctrl-C)
and you will have to use `docker stop` or `docker kill` to shutdown the batch if you want to stop it early.
