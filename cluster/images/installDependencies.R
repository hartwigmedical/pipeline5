# Use this build the R dependencies for the pipeline.  It is not an automated part of image creation to keep the imaging time down
# but can be invoked manually when the dependencies are in need of updating. It takes an hour or more to run.
#
# Start with a "clean" VM from the imaging configuration, that has been created without the extraction of the existing R
# dependencies. You can get this just by removing the extract of the R tarball that came from the resources.  Then just run this with
# `Rscript` and when it succeeds run `cd /; tar cvf rlibs.tar /usr/lib/lib/R/site-library` and replace the tarball in the tools bucket.
#
# To determine the versions of the R libraries that have been installed you could do something like this:
#   * Get a VM running from our standard image and extract the tarball of the R libraries to `/usr/local/lib/R/site-library`
#   * Run the listing script from this directory: `Rscript ./listInstalledLibs.R`
#
# We make the assumption that any libraries installed in the R library search path (try `.libPaths()` from an R shell) will not
# contain anything other than what has been packaged with the in-use R distribution. We do not use a custom library path via
# `R_LIBS_USER` because it complicates all R client programs.

install.packages("BiocManager")
install.packages("devtools")
library(BiocManager)
library(devtools)

install.packages("dplyr")
install.packages("ggplot2", update = T, ask = F)
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

install.packages("testthat", update = T, ask = F)
install.packages("stringdist", update = T, ask = F)
install.packages("assertthat", update = T, ask = F)

