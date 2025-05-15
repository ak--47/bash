#!/bin/bash

# parquet2ndjson.sh - Convert Parquet files to newline-delimited JSON using DuckDB
# Usage: ./parquet2ndjson.sh <input_path> [max_parallel_jobs] [-s|--single-file [output_filename]]

# USAGE

# # Convert a single file
# ./parquet2ndjson.sh path/to/file.parquet

# # Convert all Parquet files in a directory with default parallelism
# ./parquet2ndjson.sh path/to/directory/

# # Convert with specified parallelism (e.g., 8 parallel jobs)
# ./parquet2ndjson.sh path/to/directory/ 8

# # Convert all files in directory to a single output file
# ./parquet2ndjson.sh path/to/directory/ -s

# # Convert all files in directory to a single output file with custom name
# ./parquet2ndjson.sh path/to/directory/ -s output.ndjson

# # Convert with custom parallelism and to a single file
# ./parquet2ndjson.sh path/to/directory/ 8 -s combined.ndjson

set -e

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
    echo "Error: DuckDB is not installed. Please install it first."
    echo "Visit https://duckdb.org/docs/installation/ for installation instructions."
    exit 1
fi

# Default values
MAX_PARALLEL_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
SINGLE_FILE=false
OUTPUT_FILENAME=""

# Parse arguments
INPUT_PATH=""
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--single-file)
            SINGLE_FILE=true
            if [[ $# -gt 1 && ! $2 == -* ]]; then
                OUTPUT_FILENAME="$2"
                shift # Past the output filename
            fi
            shift # Past the argument
            ;;
        -h|--help)
            echo "Usage: $0 <input_path> [max_parallel_jobs] [-s|--single-file [output_filename]]"
            echo "  <input_path>: Path to a single Parquet file or directory containing Parquet files"
            echo "  [max_parallel_jobs]: Optional - Maximum number of parallel jobs (default: number of CPU cores)"
            echo "  -s, --single-file [output_filename]: Optional - Combine all output into a single NDJSON file"
            echo "     If no output filename is provided, it will use input_path.ndjson"
            exit 0
            ;;
        *)
            POSITIONAL_ARGS+=("$1") # Save positional args
            shift # Past argument
            ;;
    esac
done

# Restore positional parameters
set -- "${POSITIONAL_ARGS[@]}"

# Check for required arguments
if [ "$#" -lt 1 ]; then
    echo "Error: Missing input path"
    echo "Usage: $0 <input_path> [max_parallel_jobs] [-s|--single-file [output_filename]]"
    exit 1
fi

INPUT_PATH="$1"

# Set max parallel jobs if provided
if [ "$#" -ge 2 ]; then
    if [[ "$2" =~ ^[0-9]+$ ]]; then  # Check if it's a number
        MAX_PARALLEL_JOBS="$2"
    fi
fi

echo "🚀 Starting Parquet to NDJSON conversion with max $MAX_PARALLEL_JOBS parallel jobs"

# Set default output filename for single file mode if not specified
if [ "$SINGLE_FILE" = true ] && [ -z "$OUTPUT_FILENAME" ]; then
    if [ -d "$INPUT_PATH" ]; then
        # Use directory name for output
        dir_name=$(basename "$INPUT_PATH")
        OUTPUT_FILENAME="${dir_name}.ndjson"
    else
        # Use file name for output
        filename=$(basename "$INPUT_PATH" .parquet)
        OUTPUT_FILENAME="${filename}.ndjson"
    fi
fi

# Create a temporary directory for intermediate files
TEMP_DIR=""
if [ "$SINGLE_FILE" = true ]; then
    TEMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TEMP_DIR"' EXIT  # Clean up temp dir on exit
fi

# Function to convert a single parquet file to NDJSON
convert_file() {
    local input_file="$1"
    local single_file="$2"
    local temp_dir="$3"
    
    if [ "$single_file" = false ]; then
        # Individual file mode - each parquet gets its own output
        local local_output="${input_file%.*}.ndjson"
        echo "Converting $input_file to $local_output"
        
        duckdb -c "
        COPY (SELECT * FROM read_parquet('$input_file')) 
        TO '$local_output' (FORMAT JSON);
        "
        
        echo "✅ Completed: $local_output"
    else
        # Single file mode - create a temporary output file for this parquet
        local basename=$(basename "$input_file" .parquet)
        local temp_output="$temp_dir/${basename}_temp.ndjson"
        echo "Processing $input_file for combined output"
        
        duckdb -c "
        COPY (SELECT * FROM read_parquet('$input_file')) 
        TO '$temp_output' (FORMAT JSON);
        "
    fi
}

export -f convert_file

# Process files based on input type
if [ -d "$INPUT_PATH" ]; then
    echo "Processing directory: $INPUT_PATH"
    
    # Find all parquet files in the directory
    parquet_files=$(find "$INPUT_PATH" -type f -name "*.parquet")
    file_count=$(echo "$parquet_files" | wc -l | xargs)
    
    if [ "$file_count" -eq 0 ]; then
        echo "No Parquet files found in directory $INPUT_PATH"
        exit 1
    fi
    
    if [ "$SINGLE_FILE" = true ]; then
        echo "Combining all files into a single output: $OUTPUT_FILENAME"
        
        # Process all files in parallel, creating temp files
        echo "$parquet_files" | xargs -I{} -P "$MAX_PARALLEL_JOBS" bash -c 'convert_file "$1" true "$2"' _ {} "$TEMP_DIR"
        
        # Combine all temp files into the single output file
        cat "$TEMP_DIR"/*.ndjson > "$OUTPUT_FILENAME"
        
        echo "✅ Combined output completed: $OUTPUT_FILENAME"
    else
        # Process each file individually
        echo "$parquet_files" | xargs -I{} -P "$MAX_PARALLEL_JOBS" bash -c 'convert_file "$1" false ""' _ {}
    fi
    
    echo "🎉 All Parquet files in directory $INPUT_PATH have been converted to NDJSON"
elif [ -f "$INPUT_PATH" ] && [[ "$INPUT_PATH" == *.parquet ]]; then
    echo "Processing single file: $INPUT_PATH"
    
    if [ "$SINGLE_FILE" = true ]; then
        # For a single file, we can process directly to the output
        echo "Converting $INPUT_PATH to $OUTPUT_FILENAME"
        duckdb -c "
        COPY (SELECT * FROM read_parquet('$INPUT_PATH')) 
        TO '$OUTPUT_FILENAME' (FORMAT JSON);
        "
        echo "✅ Output saved to: $OUTPUT_FILENAME"
    else
        convert_file "$INPUT_PATH" false ""
    fi
    
    echo "🎉 Parquet file $INPUT_PATH has been converted to NDJSON"
else
    echo "Error: $INPUT_PATH is not a valid Parquet file or directory"
    exit 1
fi

echo "💯 Conversion complete! All Parquet files have been converted to NDJSON format."