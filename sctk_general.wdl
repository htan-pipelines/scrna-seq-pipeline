version 1.0

workflow sctkqcWF {
	input {
		String Process
		File InputCSV
		String split
		Int numCore
		String parallelType
		String outputFormat
        String memory
        Int diskSpace = 500
        Int preemptible = 2
	}

	call parseArgs {
		input: 
			InputCSV = InputCSV
	}

	scatter (sample_name in parseArgs.sample_names) {
		call sctkqc {
		input:  
			Process = parseArgs.sample2Process[sample_name], 
			Sample = sample_name,
			genome = parseArgs.sample2Genome[sample_name], 
			dataDir = parseArgs.sample2BasePath[sample_name],
			filterCountDir = parseArgs.sample2FilterDir[sample_name], 
			rawCountDir = parseArgs.sample2RawDir[sample_name],
			filterCountFile = parseArgs.sample2CellFile[sample_name],
			rawCountFile = parseArgs.sample2RawFile[sample_name],
			split = split,
			numCore = numCore,
			parallelType = parallelType,
            outputFormat = outputFormat,
            memory = memory,
            diskSpace = diskSpace,
            preemptible = preemptible
		}
	}
}

task parseArgs {
	input {
		File InputCSV
	} 

	command {
		gsutil -m cp InputCSV .
		python3 <<CODE
		import os
		import pandas as pd

		df = pd.read_csv('~{InputCSV}', header = 0, dtype=str)
		#df['filterFolder'] = df['Location'].apply(lambda x: os.path.dirname(x) + "/filtered_feature_bc_matrix/")[0]
		#df['rawFolder'] = df['Location'].apply(lambda x: os.path.dirname(x) + "/raw_feature_bc_matrix/")[0]
		df.loc[:, ['Sample', 'BasePath']].to_csv('sample2BasePath.txt',sep="\t", index=False, header=False)
		df.loc[:, ['Sample', 'RawPath']].to_csv('sample2RawDir.txt',sep="\t", index=False, header=False)
		df.loc[:, ['Sample', 'CellPath']].to_csv('sample2CellDir.txt',sep="\t", index=False, header=False)
		df.loc[:, ['Sample', 'RawFile']].to_csv('sample2RawFile.txt',sep="\t", index=False, header=False)
		df.loc[:, ['Sample', 'CellFile']].to_csv('sample2CellFile.txt',sep="\t", index=False, header=False)
		df.loc[:, ['Sample', 'Genome']].to_csv('sample2Genome.txt',sep="\t", index=False, header=False)
		df.loc[:, ['Sample', 'Process']].to_csv('sample2Process.txt',sep="\t", index=False, header=False)
		df['Sample'].to_csv('sampleName.txt', index=False, header=False)
		CODE
	}

	output {
		Array[String] sample_names = read_lines('sampleName.txt')
		Map[String, String] sample2BasePath = read_map('sample2BasePath.txt')
		Map[String, String] sample2FilterDir = read_map('sample2CellDir.txt')
		Map[String, String] sample2RawDir = read_map('sample2RawDir.txt')
		Map[String, String] sample2RawFile = read_map('sample2RawFile.txt')
		Map[String, String] sample2CellFile = read_map('sample2CellFile.txt')
		Map[String, String] sample2Genome = read_map('sample2Genome.txt')
		Map[String, String] sample2Process = read_map('sample2Process.txt')

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
		String? filterCountDir
		String? rawCountDir
		String? dataDir
		String? genome
		String? rawCountFile
		String? filterCountFile
		String split
		String AnalysisMode
		String DetectCell
		Int numCore
        String memory
		String parallelType
		String outputFormat
		String outputDir
		String Script = "/SCTK_docker/script/SCTK_runQC.R"
		String diskSpace
		Int preemptible
	}

	command {
        set -euo pipefail
        mkdir ./~{Sample}_QCOut

        python3 <<CODE
        import os
        if '~{filterCountDir}' != '':
        	os.system("gsutil -q -m cp -r ~{filterCountDir} .")
        if '~{rawCountDir}' != '':
        	os.system("gsutil -q -m cp -r ~{rawCountDir} .")
        if '~{dataDir}' != '':
        	os.system("gsutil -q -m cp -r ~{dataDir} .")
        if '~{rawCountFile}' != '':
        	os.system("gsutil -q -m cp -r ~{rawCountFile} .")
        if '~{filterCountFile}' != '':
        	os.system("gsutil -q -m cp -r ~{filterCountFile} .")        	
        

        ### run sctk-qc pipeline
        if (Process == "CellRangerV3" or Process == "CellRangerV2") and data_directory == '':
	        command = '''
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
	        '''

		elif Process == "SceRDS" or Process == "CountMatrix":
        	rawCountFile = os.path.basename('~{rawCountFile}')
        	filterCountFile = os.path.basename('~{filterCountFile}')

        	command = '''
	        Rscript SCTK_runQC.R \
	        -P ~{Process} \
	        -s ~{Sample} \
	        -r %s \
	        -c %s \
	        -o ./~{Sample}_QCOut \
	        -S ~{split} \
	        -d ~{AnalysisMode} \
	        -n ~{numCore} \
	        -T ~{parallelType} \
	        -F ~{outputFormat}
	        ''' % (rawCountFile, filterCountFile)
        
        else:
        	baseFolder = os.path.basename('~{dataDir}')
	        command = '''
	        Rscript SCTK_runQC.R \
	        -P ~{Process} \
	        -s ~{Sample} \
	        -b %s \
	        -g ~{genome} \
	        -o ./~{Sample}_QCOut \
	        -S ~{split} \
	        -d ~{AnalysisMode} \
	        -n ~{numCore} \
	        -T ~{parallelType} \
	        -F ~{outputFormat}
	        ''' % (baseFolder)

		os.system(command)
        CODE

        mv ./*.html ./~{Sample}_QCOut
        gsutil -m cp -r ./~{Sample}_QCOut ~{outputDir}
	}
	output {      
		String sctkQC_output = "~{outputDir}"
	}
	runtime {    
		# Use this container, pull from DockerHub   
		docker: "campbio/sctk_qc:2.0.0"
        cpu: numCore
        memory: memory
        disks: "local-disk ~{diskSpace} HDD"
        bootDiskSizeGb: 30
    	preemptible: preemptible
	} 
}