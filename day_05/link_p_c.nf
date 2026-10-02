#!/usr/bin/env nextflow

process SPLITLETTERS {
    input:
    val(row)

    output:
    tuple val(row), path("chunk_*")

    script:
    """
    python3 <<EOF
import textwrap

input_str = "${row.input_str}"
block_size = ${row.block_size}
out_name = "${row.out_name}"

chunks = textwrap.wrap(input_str, block_size, break_long_words=False, replace_whitespace=False)
# If textwrap doesn't split correctly (no spaces), fall back to fixed size
if len(chunks) == 1 and len(chunks[0]) > block_size:
    chunks = [input_str[i:i+block_size] for i in range(0, len(input_str), block_size)]

for i, chunk in enumerate(chunks):
    with open(f"chunk_{out_name}_{i}.txt", "w") as f:
        f.write(chunk)
EOF
    """
}

process CONVERTTOUPPER {
    input:
    tuple val(row), path(chunk_files)

    output:
    path("upper_*")

    script:
    """
    for chunk in $chunk_files; do
        cat \$chunk | tr '[:lower:]' '[:upper:]' > upper_\$(basename \$chunk)
    done
    """
}

workflow { 
    // 1. Read in the samplesheet (samplesheet_2.csv)  into a channel. The block_size will be the meta-map
    // 2. Create a process that splits the "in_str" into sizes with size block_size. The output will be a file for each block, named with the prefix as seen in the samplesheet_2
    // 4. Feed these files into a process that converts the strings to uppercase. The resulting strings should be written to stdout

    // read in samplesheet
    samplesheet_ch = channel.fromPath('samplesheet_2.csv')
        .splitCsv(header: true)
        .map { row ->
            [block_size: row.block_size as int, input_str: row.input_str, out_name: row.out_name]
        }

    // split the input string into chunks
    split_ch = SPLITLETTERS(samplesheet_ch)

    // lets remove the metamap to make it easier for us, as we won't need it anymore

    // convert the chunks to uppercase and save the files to the results directory
    upper_ch = CONVERTTOUPPER(split_ch)

    upper_ch
        .map { file -> file.text }
        .collect()
        .subscribe { lines ->
            new File('results/uppercase_chunks.txt').write(lines.join('\n'))
        }
}