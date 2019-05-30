# This can be used to build the R dependencies used by various components in the pipeline.  It is not an automated part of the 
# image creation process but rather is intended to be invoked manually when the dependencies are in need of updating. The idea is
# to build them all in one directory to allow that to be archived in a GCP bucket and pulled down to the VMs quickly. This also
# speeds up disk image creation because the R libs take about an hour to build.
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
install.packages("tidyverse", update = T, ask = F)
install.packages("rlang", update = T, ask = F)
install.packages("R6", update = T, ask = F)

BiocManager::install("VariantAnnotation")
BiocManager::install("BSgenome.Hsapiens.UCSC.hg19")
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

# As of May 2019 some symbols have been stripped from the latest mirrored version of this library so build one from Github that
# still has them intact.
devtools::install_github("PapenfussLab/StructuralVariantAnnotation", ref="pre_bioconductor")

