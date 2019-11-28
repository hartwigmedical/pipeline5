# Hartwig Medical Foundation - Batch5

This is a separate entry point to be used for building out batch operations that can be run in the cloud against large input sets
without too much work. It is expected that it will be mostly used for making one-time operations easier, rather than for repeated
invocations or unattended, pipeline-style automation.

To use the batch framework you author a batch descriptor file which contains your input URLs and a batch operation, which is a
Java class that does the actual work. By design the interfaces are very fluid and loose to allow the widest variety of tasks to be
accomplished.

When you run the framework it will invoke your batch operation once for every line in your input file, maintain instances to keep
up the desired parallelism, log progress and produce a report at the end of execution.

## Environment Setup and the Batch Descriptor

The batch needs to be run with credentials for a service account that can both read from the bucket that is referenced in the
batch descriptor and write to the chosen output bucket. It also needs to be able to create and run VM instances in the chosen
project. Given bucket names are globally unique in GCP the buckets do not necessarily have to be in the same project. 

The batch descriptor is a text file that contains the inputs that will each be run in a new VM. Typically the inputs will be GCP
storage URLs (`gs://bucket/path/...`). The full path to the batck descriptor will be passed on the command line at runtime.

## Writing Batch Operations

Operations are written in Java so you'll need a development environment of some sort. To create a new operation, implement the
`BatchOperation` interface. You are free to do most anything in the implementation. In the execution method you'll be writing code
that implements the creation of a startup script that will be executed on the VM when it starts up, parameterised with the details
for each input. All of the `BashCommand` implementations from the pipeline project are available for use here so you should not
have to do too much wheel re-creation.

A typical batch operation will download some inputs, do some processing and then write something back up to the cloud, but your
operation may be different. The interfaces have been intentionally kept quite loose to allow the widest range of problems to be
solved.

Make sure your operation has a useful "short name" and description. The short name will be used on the command line to invoke your
batch and the coordinator will begin a search for an operation that implements the requested action.

## Invocation

There is a Docker container for the batch framework that will only be useful if the operation you're trying to run has already
been implemented. If you're writing your own one-off operation you will probably want to run from within your IDE. Regardless, the
process will require a few arguments to be able to do anything. As of this writing the entry point is `BatchDispatcher` but of
course that is prone to change; it's probably good to locate the `Dockerfile` for the project and follow the breadcrumbs to find
the entry point if it has changed when you read this.

From the entry point you can locate the arguments parsing logic to find what you need to pass through. Enumerating here is
probably not that useful as the project is still young and likely to drift.

