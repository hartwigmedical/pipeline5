## What is this?

These are scripts and data for creating new base images for use in running PipelineV5 VMs on GCP. Instances are controlled via the
code in this repository. 

## Usage

1. Amend the `tools.cmds` and/or `base.cmds` files. If you are only creating a custom image, provide the `-f` argument to read
   overrides out of a bucket.
1. *If you are updating R dependencies*, probably best to rebuild them on a clean VM:
     * Create a new VM from the existing standard image
     * Amend the `installDependencies.R` file, copy it to the VM, run it. This will take quite some time probably.
     * `tar` up the contents of the `R` system libraries and push those to the bucket. To be safe include the contents of all the
         libraries, find them by running `.libPaths()` at an R prompt, eg:
         `tar cvf /data/rlibs-merged.tar /usr/local/lib/R/site-library /usr/lib/R/site-library /usr/lib/R/library`
1. Run `create_public_image.sh` to make the image.
1. When done delete the VM instance that is created for imaging as the script uses a static name for the VM and will fail fast
   if the VM already exists when it is next invoked.

## Notes

* Successive runs of the script produce a new image in the same family including a timestamp in the name. As of this writing our
  VM bootstrap code will create a VM from the latest image in the family (based upon its creation time, not its filename, though
  both should be the same image).
* There is also a R dependencies installation script here, used to build all the R dependencies needed by various components. It 
    can be manually invoked on a VM if/when the R libraries need to be updated. Further usage notes are in the script itself.
* GCP will automatically resize the filesystem of a created instance to the size requested at *VM* creation time, and this does not 
    depend on the size of the image its filesystem is being initialised from. This works at least for the images based on the Debian 
    9 series that we're using and maybe others. 
