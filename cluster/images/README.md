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

## Usage

1. Amend the `standard.cmds` file.
1. Run the `create_custom_image.sh` script to generate a shell script that you can run, passing your new commands file as the only
   argument. You may decide to redirect the output to a file, or run it directly by piping the output thru `sh`.
1. If you know you're not going to use it, delete the VM instance as the script uses a static name for the VM and will fail fast
   if the VM already exists when it is next invoked.

## Notes

* Successive runs of the script produce a new image in the same family, currently including a timestamp in the name. As of this
    writing our VM bootstrap code will create a VM from the latest image in the family (based upon its creation time, not its
    filename, though both should be the same image).
* The script leaves the imaging VM around in case you want to make any adjustments. If the VM is present when it is called the 
    script will fail fast.
* There is also a R dependencies installation script here, used to build all the R dependencies needed by various components. It 
    can be manually invoked on a VM if/when the R libraries need to be updated. Further usage notes are in the script itself.
* GCP will automatically resize the filesystem of a created instance to the size requested at *VM* creation time, and this does not 
    depend on the size of the image its filesystem is being initialised from. This works at least for the images based on the Debian 
    9 series that we're using and maybe others. 
