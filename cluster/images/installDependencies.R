# Use this build the R dependencies for the pipeline.  It is not an automated part of image creation to keep the imaging time down
# but can be invoked manually when the dependencies are in need of updating. It takes an hour or more to run.
#
# Once built the dependencies are `tar`red up and placed in the "tools" bucket on GCP we use for imaging. See `package.cmds` for
# the current bucket name. The tarball is extracted at image creation time so if any R dependencies have to be updated a new
# tarball must be created and pushed to the bucket and then the regular imaging script run. A general approach might be:
#
#   1. Start a VM and make sure `/usr/local/lib/R/site-library` is empty. An easy way to start with a clean VM with the right
#      versions of system software is to start with one of our pipeline images then remove the contents of
#      `/usr/local/lib/R/site-library`. MAKE SURE the directory itself exists though or R will silently ignore it!
#   2. Confirm `/usr/local/lib/R/site-library` is first in the output of `.libPaths()` from an R shell
#   3. Run this script with `Rscript`
#   4. Make sure `/usr/local/lib/R/site-library` was populated as expected
#   5. Make a new tarball: `cd /; tar cvf rlibs.tar /usr/local/lib/R/site-library`
#   6. Create a backup of the existing tarball in the tools bucket, then copy the new tarball over the original
#   7. Re-run the imaging script and verify installed versions of R libs. See `./listInstalledLibs.R`.
#
# We make the assumption that any libraries installed in the R library search path (try `.libPaths()` from an R shell) will not
# contain anything other than what has been packaged with the in-use R distribution. We do not use a custom library path via
# `R_LIBS_USER` because it complicates all R client programs.

install.packages("BiocManager")
library(BiocManager)

install.packages("dplyr")
install.packages("ggplot2", update = T, ask = F)
install.packages("patchwork", update = T, ask = F)
install.packages("ggh4x", update = T, ask = F)
install.packages("stringr", update = T, ask = F)
install.packages("magick", update = T, ask = F)
install.packages("VariantAnnotation", update = T, ask = F)
install.packages("copynumber", update = T, ask = F)
install.packages("cowplot", update = T, ask = F)

install.packages("argparser", update = T, ask = F)
install.packages("XML", update = T, ask = F)
install.packages("rtracklayer", update = T, ask = F)
install.packages("BSgenome", update = T, ask = F)
install.packages("BSgenome.Hsapiens.UCSC.hg19", update = T, ask = F)
install.packages("BSgenome.Hsapiens.UCSC.hg38", update = T, ask = F)
install.packages("tidyverse", update = T, ask = F)
install.packages("rlang", update = T, ask = F)
install.packages("R6", update = T, ask = F)

BiocManager::install("VariantAnnotation")
BiocManager::install("StructuralVariantAnnotation")
BiocManager::install("BSgenome.Hsapiens.UCSC.hg19")
BiocManager::install("BSgenome.Hsapiens.UCSC.hg38")
BiocManager::install("BiocGenerics")
BiocManager::install("S4Vectors")
BiocManager::install("IRanges")
BiocManager::install("GenomeInfoDb")
BiocManager::install("GenomicRanges")
BiocManager::install("Biostrings")
BiocManager::install("Rsamtools")
BiocManager::install("GenomicAlignments")
BiocManager::install("Gviz")

install.packages("testthat", update = T, ask = F)
install.packages("stringdist", update = T, ask = F)
install.packages("assertthat", update = T, ask = F)

