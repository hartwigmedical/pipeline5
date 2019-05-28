# This can be used to build the R dependencies used by various components in the pipeline.  It is not an automated part of the 
# image creation process but rather is intended to be invoked manually when the dependencies are in need of updating. The idea is
# to build them all in one directory to allow that to be archived in a GCP bucket and pulled down to the VMs quickly. This also
# speeds up disk image creation because the R libs take about an hour to build.
#
# The goal of this script is to get the dependencies installed such that the stuff will run. Not objective beauty.

install.packages("BiocManager", lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("devtools", lib="/data/tools/gridss/2.2.2/rlibs")
library(BiocManager)
library(devtools)

install.packages("dplyr", lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("ggplot2", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("VariantAnnotation", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("copynumber", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")

install.packages("argparser", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("XML", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("rtracklayer", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("BSgenome", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("BSgenome.Hsapiens.UCSC.hg19", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("tidyverse", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")

BiocManager::install("VariantAnnotation", lib="/data/tools/gridss/2.2.2/rlibs")
BiocManager::install("BSgenome.Hsapiens.UCSC.hg19", lib="/data/tools/gridss/2.2.2/rlibs")

install.packages("testthat", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("stringdist", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")
install.packages("assertthat", update = T, ask = F, lib="/data/tools/gridss/2.2.2/rlibs")

# As of May 2019 some symbols have been stripped from the latest mirrored version of this library so build one from Github that
# still has them intact.
devtools::install_github("PapenfussLab/StructuralVariantAnnotation", ref="pre_bioconductor", lib="/data/tools/gridss/2.2.2/rlibs")

