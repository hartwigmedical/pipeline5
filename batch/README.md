# Hartwig Medical Foundation - Batch5

This is a separate entry point to be used for building out batch operations that can be run in the cloud against large input sets
without too much work. It is expected that it will be mostly used for making one-time operations easier, rather than for repeated
invocations or unattended, pipeline-style automation.

This guide is written for developers so a certain amount of expertise is assumed. The application is written in Java.

To use the batch framework you author a batch descriptor file which contains your input URLs and a batch operation, which is a
Java class that does the actual work. By design the interfaces are very fluid and loose to allow the widest variety of tasks to be
accomplished.

When you run the framework it will invoke your batch operation once for every input in your file, maintain instances to keep
up the desired parallelism, log progress and produce a report at the end of execution.

To use this framework:

- Possibly write a new operation of your own;
- Write an input file;
- Make sure the GCP environment is configured;
- Run your operation in a live environment.

## Writing Batch Operations

This is relevant if you want to write a custom batch operation. There are several operations already built in the project (search
for implementors of the `BatchOperation` interface), one of which may suit your purposes and save your writing another.

If you do need to write your own operation there are two distinct approaches:

* *Check out the code and add your new operation*. Suitable if you're an HMF contributor and your operation will be committed back
    into the project when you're done.
* *Add the project as a dependency in Maven and implement locally*. If you just want to take advantage of the framework to run an
    operation that you define this may be easiest and quickest as you will not need to submit and wait for a pull request.

In both cases you will need to create a new implementation of the `BatchOperation` interface. Make sure the operation name
returned in the `BatchDescriptor` does not collide with an existing operation, and succintly describes your operation as it will
be used to call it on the command line.

A typical batch operation will download some inputs, do some processing and then write something back up to the cloud, but your
operation may be different. The interfaces have been intentionally kept quite loose to allow the widest range of problems to be
solved.

### Adding the Project as a Maven Dependency

The artifacts are currently hosted in a GCP bucket and you must configure your Maven to find them. In your POM:

```
<project>
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

## Writing an Input File

The format of the input file is defined by the operation. Look at the code for existing operations, but if you're writing your own
it is pretty much up to you what you put in here. There is a simple format that just runs one instance for every line in your file
and a more-powerful JSON version. The operation must return the type it will use in its descriptor.

## Confuring the GCP Environment

The batch needs to be run with credentials for a service account that can both read from the bucket that is referenced in the
batch descriptor and write to the output bucket. It also needs to be able to create and run VM instances in the chosen project.
Given bucket names are globally unique in GCP the buckets do not necessarily have to be in the same project. 

## Running your Operation

If you've written a custom operation it's easiest to start up the `Docker` container and execute your batch. If you have checked
the project out you will probably find your IDE is easiest.

### From the IDE

The entry point for the application is the `BatchDispatcher` class.

### Docker 

Our CI build publishes a `Docker` container for the batch framework under `hartwigmedicalfoundation/batch5`. In this case you need
to make the JAR file containing your class available for the `Docker` instance to pick up at runtime (the path on the Docker
container is not configurable):

```
$ cp jar-containing-operation.jar /tmp/jarfiles
$ docker run -v /tmp/jarfiles:/usr/share/thirdpartyjars hartwigmedicalfoundation/batch5 MyOp -private_key_path ...
```

