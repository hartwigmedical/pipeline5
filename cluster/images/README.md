## What is this?

These are scripts and data for creating new base images for use in running virtual machine (VM) instances on `Google Cloud
Platform (GCP)`. We are interested in running some tasks outside of the `DataProc` product; we've decided to use VM instances in
GCP for this purpose.

We want to be able to start up "disposable" VM instances at particular parts of our pipeline, in a known state and with known
software installed. The pipeline code is configured to do the actual starting and we can ignore that here; the code here is
designed to allow us to declare what VMs look like for a component and create the image for that component. The pipeline code will
create and start an instance from the image when it needs to invoke a component.

The goals are:

* _Provide a simple, declarative mechanism for creating new VM images._ We assume that we're always starting from a
    Google-provided base image. The machine "descriptions" should be easy to read and versionable.
* _Work on a UNIX-like environment with access to a BASH shell and the `gcloud` tool._
* _Take advantage of existing GCP tools, infrastructure and conventions whenever possible._

## How is this used?

1. Author a new "command file" containing those commands you want to be run against the new instance via `ssh` when it has come
   up. This might include for instance special packages required by the instance. You may include comments in this file (lines starting
   with `#`); it should act as documentation as well as code, and nothing should have to be done manually after your command file
   has been run.
1. Run the `create_custom_image.sh` script to generate a shell script that you can run, passing your new commands file as the only
   argument. You may decide to redirect the output to a file, or run it directly by piping the output thru `sh`.
1. Once you're happy with the VM image that's been created you may manually clean up the VM that was created to build it. The
   script does not reap the VM automatically (by design). 

Note that the resultant image will be named according to the commands file you've passed to it. Any subsequent invocations of the
same commands file will result in a newer version of the image with the same family. Code creating instances from the images may
specify either a particular version of the image, or just the family, which will result in the newest member of that family being
selected.

