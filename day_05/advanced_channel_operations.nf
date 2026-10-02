params {
    step: Integer = 0
}

workflow {

    // Task 1 - Read in the samplesheet.
    if (params.step == 1) {
        channel.fromPath('samplesheet.csv')
            .splitCsv(header: true)
            .view()
    }

    // Task 2 - Read in the samplesheet and create a meta-map with all metadata and another list with the filenames ([[metadata_1 : metadata_1, ...], [fastq_1, fastq_2]]).
    //          Set the output to a new channel "in_ch" and view the channel. YOU WILL NEED TO COPY AND PASTE THIS CODE INTO SOME OF THE FOLLOWING TASKS (sorry for that).
    if (params.step == 2) {
        in_ch = channel.fromPath('samplesheet.csv')
            .splitCsv(header: true)
            .map { row ->
                def meta = row.findAll { entry -> entry.key != 'fastq_1' && entry.key != 'fastq_2' }
                def files = [row.fastq_1, row.fastq_2]
                [meta, files]
            }
        in_ch.set { in_ch }
        in_ch.view()    
    }

    // Task 3 - Now we assume that we want to handle different "strandedness" values differently. 
    //          Split the channel into the right amount of channels and write them all to stdout so that we can understand which is which.
    if (params.step == 3) {
        in_ch = channel.fromPath('samplesheet.csv')
            .splitCsv(header: true)
            .map { row ->
                def meta = row.findAll { entry -> entry.key != 'fastq_1' && entry.key != 'fastq_2' }
                def files = [row.fastq_1, row.fastq_2]
                [meta, files]
            }
        in_ch.set { in_ch }

        // Use branch to split by strandedness
        def stranded_ch = in_ch.branch {
            auto: it[0].strandedness == 'auto'
            forward: it[0].strandedness == 'forward'
            reverse: it[0].strandedness == 'reverse'
        }
        stranded_ch.auto.view { "STRANDEDNESS=auto: $it" }
        stranded_ch.forward.view { "STRANDEDNESS=forward: $it" }
        stranded_ch.reverse.view { "STRANDEDNESS=reverse: $it" }
    }

    // Task 4 - Group together all files with the same sample-id and strandedness value.
    if (params.step == 4) {
        in_ch = channel.fromPath('samplesheet.csv')
            .splitCsv(header: true)
            .map { row ->
                def meta = row.findAll { entry -> entry.key != 'fastq_1' && entry.key != 'fastq_2' }
                def files = [row.fastq_1, row.fastq_2]
                [meta, files]
            }
        in_ch.set { in_ch }

        in_ch
            .map { def key = [it[0].sample, it[0].strandedness]; [key, it[1]] }
            .groupTuple()
            .map { key, files_list ->
                def all_files = files_list.flatten().unique()
                def meta = [sample: key[0], strandedness: key[1]]
                [meta, all_files]
            }
            .view()    
    }
}