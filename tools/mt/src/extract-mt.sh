#!/usr/bin/env bash

set -euo pipefail

usage() {
    echo "Usage: $0 --cram <cram_file> --output-dir <output_directory> --sample-id <sample_identifier> [--threads <num_threads>]"
    echo
    echo "Required arguments:"
    echo "  --cram        Local path to CRAM file"
    echo "  --output-dir  Local directory where output files should be placed"
    echo "  --sample-id   Sample identifier to use in output file names"
    echo
    echo "Optional arguments:"
    echo "  --threads     number of threads to use (default: number of processors - 1)"
    exit 1
}

log() {
    local message=$1
    echo "$(date +"%Y-%m-%d %H:%M:%S") $message"
}

cram_file=""
output_dir=""
sample_id=""
threads=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --cram)
            cram_file="$2"
            shift 2
            ;;
        --output-dir)
            output_dir="$2"
            shift 2
            ;;
        --sample-id)
            sample_id="$2"
            shift 2
            ;;
        --threads)
            threads="$2"
            shift 2
            ;;
        *)
            echo "Unknown parameter: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [ -z "$cram_file" ]; then
    echo "Error: missing required argument --cram"
    usage
elif [ -z "$output_dir" ]; then
    echo "Error: missing required argument --output-dir"
    usage
elif [ -z "$sample_id" ]; then
    echo "Error: missing required argument --sample-id"
    usage
fi

# Validate CRAM file exist
if [ ! -f "$cram_file" ]; then
    echo "Error: CRAM file does not exist: $cram_file"
    exit 1
fi

# Validate number of threads
if [[ -n "$threads" ]]; then
    if ! [[ "$threads" =~ ^[0-9]+$ ]] || [ "$threads" -lt 1 ]; then
        echo "Error: threads must be a positive integer"
        exit 1
    fi
    thread_count="$threads"
else
    thread_count=$(($(nproc) - 1))
fi

OUTPUT_TMP_DIR="/tmp/output"
OUTPUT_READ_NAMES="$OUTPUT_TMP_DIR/${sample_id}.readnames.txt"
OUTPUT_UNSORTED_BAM="$OUTPUT_TMP_DIR/${sample_id}.mt.unsorted.bam"
OUTPUT_SORTED_BAM="$OUTPUT_TMP_DIR/${sample_id}.mt.bam"

# determine the chromosome name to find
if samtools view -H "$cram_file" | grep @SQ | grep SN:MT >/dev/null
then
  # CRAM aligned with HG37
  chromosome=MT
else
  # CRAM aligned with HG38
  chromosome=chrM
fi

log "Extracting mitochondrial DNA from $cram_file"
log "  chromosome name:  $chromosome"
log "  threads used:     $thread_count"
log "  output stored at: $output_dir"
log "  sample id:        $sample_id"

if [ ! -d "$output_dir" ]; then
    log "Creating output directory"
    mkdir -p "$output_dir"
fi

log "Creating local output directory..."
mkdir -p "$OUTPUT_TMP_DIR"

log "Extracting mitochondrial read names, step 1 of 2..."
samtools view --threads "$thread_count" "$cram_file" "*" | (grep "$chromosome" || echo -n "") | cut -f 1 > "$OUTPUT_READ_NAMES"

log "Extracting mitochondrial read names, step 2 of 2..."
samtools view --threads "$thread_count" "$cram_file" "$chromosome"  | cut -f 1 >> "$OUTPUT_READ_NAMES"

log "Fetching pairs..."
samtools view --fetch-pairs -b -N "$OUTPUT_READ_NAMES" -@ "$thread_count" "$cram_file" "*" > "$OUTPUT_UNSORTED_BAM"

log "Sorting BAM..."
samtools sort -@ "$thread_count" -o "$OUTPUT_SORTED_BAM" "$OUTPUT_UNSORTED_BAM"

log "Creating BAM index..."
samtools index -@ "$thread_count" "$OUTPUT_SORTED_BAM"

log "Copying output files..."
cp $OUTPUT_TMP_DIR/* "$output_dir"

log "Done"
