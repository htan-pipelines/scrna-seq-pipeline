version 1.0

workflow sctkqcWF {
	input {
		String Process
		File input_csv
		String split
		Int numCore
		String parallelType
		String outputFormat
        String memory
        Int disk_space = 500
        Int preemptible = 2
	}

	call parseCSV {
		input: 
			input_csv = input_csv
	}

	scatter (sample_name in parseCSV.sample_names) {
		call sctkqc {
		input:  
			Process = "CellRangerV3", 
			Sample = sample_name,
			filterCount_directory = parseCSV.sample2FilterDir[sample_name], 
			rawCount_directory = parseCSV.sample2RawDir[sample_name],
			split = split,
			numCore = numCore,
			parallelType = parallelType,
            outputFormat = outputFormat,
            memory = memory,
            disk_space = disk_space,
            preemptible = preemptible
		}
	}
}

task parseCSV {
	input {
		String? input_csv
	} 

	command {
		gsutil -m cp input_csv .

		python3 <<CODE
		import os
		import pandas as pd

		df = pd.read_csv('~{input_csv}', header = 0, dtype=str)
		df['filterFolder'] = df['Location'].apply(lambda x: os.path.dirname(x) + "/filtered_feature_bc_matrix/")[0]
		df['rawFolder'] = df['Location'].apply(lambda x: os.path.dirname(x) + "/raw_feature_bc_matrix/")[0]
		df.loc[:, ['Sample', 'rawFolder']].to_csv('sample2RawDir.txt',sep="\t", index=False, header=False)
		df.loc[:, ['Sample', 'filterFolder']].to_csv('sample2FilterDir.txt',sep="\t", index=False, header=False)
		df['Sample'].to_csv('sampleName.txt', index=False, header=False)
		CODE
	}

	output {
		Array[String] sample_names = read_lines('sampleName.txt')
		Map[String, String] sample2FilterDir = read_map('sample2FilterDir.txt')
		Map[String, String] sample2RawDir = read_map('sample2RawDir.txt')
	}
	runtime {    
		# Use this container, pull from DockerHub   
		docker: "campbio/sctk_qc:2.0.0"    
	} 
}

task sctkqc {
	input {
		String Process
		String Sample
		String filterCount_directory
		String rawCount_directory
		String split
		String AnalysisMode
		Int numCore
        String memory
		String parallelType
		String outputFormat
		String output_directory
		String Script = "/SCTK_docker/script/SCTK_runQC.R"
		String disk_space
		Int preemptible
	}

	command {
        set -euo pipefail
        gsutil -q -m cp -r ~{filterCount_directory} .
        gsutil -q -m cp -r ~{rawCount_directory} .
        gsutil -q -m cp ~{Script} .	
        mkdir ./~{Sample}_QCOut
        #touch ./~{Sample}_QCOut/test.txt

        #echo $(ls ./filtered_feature_bc_matrix) >> ./~{Sample}_QCOut/test.txt
        #echo $(ls ./raw_feature_bc_matrix) >> ./~{Sample}_QCOut/test.txt
        #echo $(ls .) >> ./~{Sample}_QCOut/test.txt
        #echo "${Sample} \n" >> ./~{Sample}_QCOut/test.txt

        Rscript SCTK_runQC.R \
        -P ~{Process} \
        -s ~{Sample} \
        -R ./raw_feature_bc_matrix \
        -C ./filtered_feature_bc_matrix \
        -o ./~{Sample}_QCOut \
        -S ~{split} \
        -d ~{AnalysisMode} \
        -n ~{numCore} \
        -T ~{parallelType} \
        -F ~{outputFormat}

        mv ./*.html ./~{Sample}_QCOut
        gsutil -m cp -r ./~{Sample}_QCOut ~{output_directory}
	}
	output {      
		String sctkQC_output = "~{output_directory}"
	}
	runtime {    
		# Use this container, pull from DockerHub   
		docker: "campbio/sctk_qc:2.0.0"
        cpu: numCore
        memory: memory
        disks: "local-disk ~{disk_space} HDD"
        bootDiskSizeGb: 30
    	preemptible: preemptible
	} 
}