#!/usr/bin/env bash

# parquet-to.sh - Convert Parquet files to NDJSON, Parquet, or CSV using DuckDB
# Usage: ./parquet-to.sh <input_path> [max_parallel_jobs] [-s|--single-file [output_filename]] [-f|--format <ndjson|parquet|csv>]

set -euo pipefail

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
    echo "Error: duckdb is not installed. Please install it first."
    echo "Visit https://duckdb.org/docs/installation/ for installation instructions."
    exit 1
fi

# Defaults
MAX_PARALLEL_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
SINGLE_FILE=false
OUTPUT_FILENAME=""
FORMAT="ndjson"

print_help() {
    cat <<EOF
Usage: $0 <input_path> [max_parallel_jobs] [-s|--single-file [output_filename]] [-f|--format <ndjson|parquet|csv>]

  <input_path>                Path to a single Parquet file or directory containing Parquet files
  [max_parallel_jobs]         Optional - Maximum number of parallel jobs (default: CPU cores)
  -s, --single-file           Optional - Combine all output into a single file
     [output_filename]        Optional - Name for combined output (default: based on input)
  -f, --format <ndjson|parquet|csv>
                              Optional - Output format (default: ndjson)

Examples:
  # Convert a single Parquet to NDJSON
  ./parquet-to.sh data/file.parquet

  # Convert entire directory with 8 jobs into CSV
  ./parquet-to.sh data/ 8 -f csv

  # Combine directory into one Parquet file
  ./parquet-to.sh data/ -s combined.parquet -f parquet
EOF
}

# Parse flags
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--single-file)
            SINGLE_FILE=true
            shift
            if [[ $# -gt 0 && ! $1 =~ ^- ]]; then
                OUTPUT_FILENAME="$1"
                shift
            fi
            ;;
        -f|--format)
            if [[ $# -lt 2 ]]; then
                echo "Error: --format requires an argument."
                exit 1
            fi
            FORMAT="$2"
            shift 2
            ;;
        -h|--help)
            print_help
            exit 0
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done

# Restore positional args
set -- "${POSITIONAL[@]}"

# Validate args
if [ $# -lt 1 ]; then
    echo "Error: Missing <input_path>"
    print_help
    exit 1
fi
INPUT_PATH="$1"

if [ $# -ge 2 ] && [[ "${2}" =~ ^[0-9]+$ ]]; then
    MAX_PARALLEL_JOBS="$2"
fi

# Validate format
case "$FORMAT" in
    ndjson)
        EXT="ndjson"
        COPY_OPTS="FORMAT JSON"
        ;;
    parquet)
        EXT="parquet"
        COPY_OPTS="FORMAT PARQUET"
        ;;
    csv)
        EXT="csv"
        COPY_OPTS="FORMAT CSV, HEADER"
        ;;
    *)
        echo "Error: Unsupported format '$FORMAT'. Choose ndjson, parquet, or csv."
        exit 1
        ;;
esac

# Export so child xargs/bash -c sees them
export EXT COPY_OPTS

echo "🚀 Converting with format=$FORMAT, max_parallel_jobs=$MAX_PARALLEL_JOBS, single_file=$SINGLE_FILE"

# Determine default output filename for single-file mode
if [ "$SINGLE_FILE" = true ] && [ -z "$OUTPUT_FILENAME" ]; then
    if [ -d "$INPUT_PATH" ]; then
        OUTPUT_FILENAME="$(basename "$INPUT_PATH").$EXT"
    else
        OUTPUT_FILENAME="$(basename "${INPUT_PATH%.*}").$EXT"
    fi
fi

# Prepare temp directory if needed (for ndjson/csv)
if [ "$SINGLE_FILE" = true ] && [ "$FORMAT" != "parquet" ]; then
    TEMP_DIR="./tmp"
    mkdir -p "$TEMP_DIR"
    trap 'rm -rf "$TEMP_DIR"' EXIT
fi

# Function to convert a single Parquet file
convert_file() {
    local infile="$1"
    local single="$2"
    local tmpdir="$3"
    local base="$(basename "${infile%.*}")"

    if [ "$single" = false ]; then
        local out="${infile%.*}.$EXT"
        echo "Converting $infile → $out"
        duckdb -c "COPY (SELECT * FROM read_parquet('$infile')) TO '$out' ($COPY_OPTS);"
        echo "✅ $out"
    else
        local tmpout="$tmpdir/${base}_temp.$EXT"
        echo "Processing $infile → $tmpout"
        duckdb -c "COPY (SELECT * FROM read_parquet('$infile')) TO '$tmpout' ($COPY_OPTS);"
    fi
}
export -f convert_file

# Main processing
if [ -d "$INPUT_PATH" ]; then
    echo "Scanning directory: $INPUT_PATH"
    mapfile -t files < <(find "$INPUT_PATH" -type f -name "*.parquet")
    if [ ${#files[@]} -eq 0 ]; then
        echo "No Parquet files found in $INPUT_PATH"
        exit 1
    fi

    if [ "$SINGLE_FILE" = true ]; then
        echo "Combining into single output: $OUTPUT_FILENAME"

        if [ "$FORMAT" = "parquet" ]; then
            # Build SQL array literal of file paths
            SQL_PATHS=$(printf "'%s'," "${files[@]}")
            SQL_PATHS=${SQL_PATHS%,}

            # Let DuckDB merge them in one go
            duckdb -c "COPY (
  SELECT * 
  FROM read_parquet(ARRAY[${SQL_PATHS}])
) TO '$OUTPUT_FILENAME' (FORMAT PARQUET);"

            echo "✅ Merged Parquet → $OUTPUT_FILENAME"
        else
            # Convert each to tmp and cat together
            printf '%s\n' "${files[@]}" \
              | xargs -n1 -P "$MAX_PARALLEL_JOBS" -I{} bash -c 'convert_file "$@"' _ {} true "$TEMP_DIR"
            cat "$TEMP_DIR"/*."$EXT" > "$OUTPUT_FILENAME"
            echo "✅ Combined → $OUTPUT_FILENAME"
        fi
    else
        # Individual conversion
        printf '%s\n' "${files[@]}" \
          | xargs -n1 -P "$MAX_PARALLEL_JOBS" -I{} bash -c 'convert_file "$@"' _ {} false ""
    fi

    echo "🎉 Directory conversion complete."
elif [ -f "$INPUT_PATH" ] && [[ "$INPUT_PATH" == *.parquet ]]; then
    echo "Single file mode: $INPUT_PATH"
    if [ "$SINGLE_FILE" = true ]; then
        echo "Converting → $OUTPUT_FILENAME"
        duckdb -c "COPY (SELECT * FROM read_parquet('$INPUT_PATH')) TO '$OUTPUT_FILENAME' ($COPY_OPTS);"
        echo "✅ $OUTPUT_FILENAME"
    else
        convert_file "$INPUT_PATH" false ""
    fi
    echo "🎉 File conversion complete."
else
    echo "Error: '$INPUT_PATH' is not a Parquet file or directory"
    exit 1
fi

echo "💯 All done!"
