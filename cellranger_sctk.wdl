version 1.0

import "https://api.firecloud.org/ga4gh/v1/tools/cumulus:cellranger_workflow/versions/26/plain-WDL/descriptor" as crwf
import "https://api.firecloud.org/ga4gh/v1/tools/test_manifest_file:test_manifest_file/versions/60/plain-WDL/descriptor" as HTAN_meta

workflow cellranger_sctk {
	input {
		###### cellranger parameter ######
		File input_csv_file = "coumt_matrix.csv"

		# If generate HTAN manifest files
		Boolean generate_HTAN_manifest = true
		# If run cumulus cellranger
		Boolean run_cumulus_cellranger
		# If run cellranger mkfastq
		Boolean run_mkfastq = false
		# If run cellranger count
		Boolean run_count = true

		# for mkfastq

		# Whether to delete input_bcl_directory, default: false
		Boolean delete_input_bcl_directory = false
		# Number of allowed mismatches per index
		Int? mkfastq_barcode_mismatches

		# common to cellranger count/vdj and cellranger-atac count

		# Force pipeline to use this number of cells, bypassing the cell detection algorithm, mutually exclusive with expect_cells.
		Int? force_cells

		# For count

		# Expected number of recovered cells. Mutually exclusive with force_cells
		Int? expect_cells
		# Perform secondary analysis of the gene-barcode matrix (dimensionality reduction, clustering and visualization). Default: false
		Boolean secondary = false

		# For vdj

		# Do not align reads to reference V(D)J sequences before de novo assembly. Default: false
		Boolean vdj_denovo = false

		# For extracting ADT count

		# scaffold sequence for Perturb-seq, default is "", which for Perturb-seq means barcode starts at position 0 of read 2
		String scaffold_sequence = ""
		# maximum hamming distance in feature barcodes
		Int max_mismatch = 3
		# minimum read count ratio (non-inclusive) to justify a feature given a cell barcode and feature combination, only used for data type crispr
		Float min_read_ratio = 0.1

		# For atac

		# For atac, choose the algorithm for dimensionality reduction prior to clustering and tsne: 'lsa' (default), 'plsa', or 'pca'.
		String? atac_dim_reduce


		# 4.0.0, 3.1.0, 3.0.2, 2.2.0
		String cellranger_version = "4.0.0"
		# 1.2.0, 1.1.0
		String cellranger_atac_version = "1.2.0"
		# 0.3.0, 0.2.0
		String cumulus_feature_barcoding_version = "0.3.0"
		# Which docker registry to use: cumulusprod (default) or quay.io/cumulus
		String cumulus_docker_registry = "cumulusprod"
		# cellranger/cellranger-atac mkfastq registry, default to gcr.io/broad-cumulus
		String mkfastq_docker_registry = "gcr.io/broad-cumulus"
		# Google cloud zones, default to "us-central1-a us-central1-b us-central1-c us-central1-f us-east1-b us-east1-c us-east1-d us-west1-a us-west1-b us-west1-c"
		String zones = "us-central1-a us-central1-b us-central1-c us-central1-f us-east1-b us-east1-c us-east1-d us-west1-a us-west1-b us-west1-c"
		# Number of cpus per cellranger job
		Int cellranger_num_cpu = 32
		# Memory string
		String cellranger_memory = "120G"

		# Number of cpus for cellranger-atac count
		Int atac_num_cpu = 64
		# Memory string for cellranger-atac count
		String atac_memory = "57.6G"

		# Optional memory string for cumulus_adt
		String feature_memory = "32G"
		# Optional disk space for mkfastq.
		Int mkfastq_disk_space = 1500
		# Optional disk space needed for cell ranger count.
		Int count_disk_space = 500
		# Optional disk space needed for cell ranger vdj.
		Int vdj_disk_space = 500
		# Optional disk space needed for cumulus_adt
		Int feature_disk_space = 100

		###### sctk parameter ######
		String? cumulus_output_csv
		String Process = "CellRangerV3"
		String split = "TRUE"
		Int sctk_num_cpu
		String parallel_type = "MulticoreParam"
		String output_format = "SCE,AnnData,FlatFile,HTAN"
		String sctk_memory = "100GB"
		String sctk_docker = "campbio/sctk_qc:2.2.0"
		Int sctk_disk_space = 500
		Int preemptible = 2
		Int manifest_num_cpu
		String manifest_memory
		Int manifest_disk_space
		String? genome_reference_name
		String detectMitoLevel = "TRUE"
		String mitoType = "human-ensembl"
		
		# Output directory for both cumulus and SCTK-QC, gs URL
		String cellranger_output_directory = "output_directory"
		String sctkQC_output_directory
	}

	if (run_cumulus_cellranger) {
		call crwf.cellranger_workflow as cellranger_workflow {
			input:
				input_csv_file = input_csv_file,
				output_directory = cellranger_output_directory,
				run_mkfastq = run_mkfastq,
				run_count = run_count,
				delete_input_bcl_directory = delete_input_bcl_directory,
				mkfastq_barcode_mismatches = mkfastq_barcode_mismatches,
				force_cells = force_cells,
				expect_cells = expect_cells,
				secondary = secondary,
				vdj_denovo = vdj_denovo,
				scaffold_sequence = scaffold_sequence,
				max_mismatch = max_mismatch,
				min_read_ratio = min_read_ratio,
				atac_dim_reduce = atac_dim_reduce,
				cellranger_version = cellranger_version,
				cellranger_atac_version = cellranger_atac_version,
				cumulus_feature_barcoding_version = cumulus_feature_barcoding_version,
				docker_registry = cumulus_docker_registry,
				mkfastq_docker_registry = mkfastq_docker_registry,
				zones = zones,
				num_cpu = cellranger_num_cpu,
				memory = cellranger_memory,
				atac_num_cpu = atac_num_cpu,
				atac_memory = atac_memory,
				feature_memory = feature_memory,
				mkfastq_disk_space = mkfastq_disk_space,
				count_disk_space = count_disk_space,
				vdj_disk_space = vdj_disk_space,
				feature_disk_space = feature_disk_space
		}		
	}

	
	### parse input in the input_csv for SCTK-QC workflow
	call parseCSV {
		input: 
			run_cumulus_cellranger = run_cumulus_cellranger,
			cumulus_count_matrix = cellranger_workflow.count_matrix,
			cumulus_outupt_dir = cellranger_output_directory,
			count_matrix_dir = cumulus_output_csv
	}		

	scatter (sample_name in parseCSV.sample_names) {
		call sctkqc {
		input:  
			Process = Process, 
			Sample = sample_name,
			filterCount_directory = parseCSV.sample2FilterDir[sample_name], 
			rawCount_directory = parseCSV.sample2RawDir[sample_name],
			output_directory = sctkQC_output_directory,
			split = split,
			numCore = sctk_num_cpu,
			parallelType = parallel_type,
			outputFormat = output_format,
			memory = sctk_memory,
			disk_space = sctk_disk_space,
			preemptible = preemptible,
			sctk_docker = sctk_docker,
			detectMitoLevel = detectMitoLevel,
			mitoType = mitoType
		}			
	}

	if (generate_HTAN_manifest) {
		call HTAN_meta.test_HTAN_meta as HTAN_meta {
			input: 
				cumulus_count_matrix = cellranger_workflow.count_matrix,
				user_count_matrix = cumulus_output_csv,
				run_cumulus_cellranger = run_cumulus_cellranger,
				sample_sheet = input_csv_file, 
				output_dir = sctkQC_output_directory, # sctkqc.sctkQC_output[0]
				cellranger_version = cellranger_version,
				manifest_num_cpu = manifest_num_cpu,
				manifest_memory = manifest_memory,
				manifest_disk_space = manifest_disk_space,
				genome_reference_name = genome_reference_name,
				sctk_output_dir = sctkQC_output_directory, #sctkqc.sctkQC_output[0]
				tmp = sctkqc.tmp[0],
				sctk_docker = sctk_docker
		}
	}		
}


task parseCSV {
	input {
		Boolean run_cumulus_cellranger 
		String? cumulus_count_matrix
		String? count_matrix_dir
		String cumulus_outupt_dir
		String sctk_docker = "campbio/sctk_qc:2.2.0"
	} 

	command {

		python3 <<CODE
		import os
		import pandas as pd
		print("Start parseCSV step")
		if '~{run_cumulus_cellranger}' == 'true':
			count_matrix_path = "~{cumulus_count_matrix}"
		else:
			count_matrix_path = "~{count_matrix_dir}"

		code = "gsutil -q -m cp %s ." % (count_matrix_path)
		print(code)
		os.system(code)

		### copy count matrix to cellranger output directory
		if '~{run_cumulus_cellranger}' == 'true':
			code = "gsutil -q -m cp %s ~{cumulus_outupt_dir}" % (count_matrix_path)
			print(code)
			os.system(code)

		mat_file = os.path.basename(count_matrix_path)
		df = pd.read_csv(mat_file, header = 0, dtype=str)
		df['filterFolder'] = df['Location'].apply(lambda x: os.path.dirname(x) + "/filtered_feature_bc_matrix/")
		df['rawFolder'] = df['Location'].apply(lambda x: os.path.dirname(x) + "/raw_feature_bc_matrix/")
		df.loc[:, ['Sample', 'rawFolder']].to_csv('sample2RawDir.txt',sep="\t", index=False, header=False)
		df.loc[:, ['Sample', 'filterFolder']].to_csv('sample2FilterDir.txt',sep="\t", index=False, header=False)
		df['Sample'].to_csv('sampleName.txt', index=False, header=False)
		df.to_csv('df_forDebug.csv', index=True, header=True)
		os.system("gsutil -q -m cp df_forDebug.csv ~{cumulus_outupt_dir}")
		print("Done parseCSV step")
		CODE
	}

	output {
		Array[String] sample_names = read_lines('sampleName.txt')
		Map[String, String] sample2FilterDir = read_map('sample2FilterDir.txt')
		Map[String, String] sample2RawDir = read_map('sample2RawDir.txt')
	}

	runtime {
		# Use this container, pull from DockerHub   
		docker: sctk_docker    
	} 
}

task sctkqc {
	input {
		String Process
		String Sample
		String filterCount_directory
		String rawCount_directory
		String split
		String AnalysisMode = "Cell"
		Int numCore
		String memory
		String parallelType
		String outputFormat
		String output_directory
		String Script = "/SCTK_docker/script/SCTK_runQC.R"
		String disk_space
		Int preemptible
		String sctk_docker = "campbio/sctk_qc:2.2.1"
		String detectMitoLevel
		String mitoType
	}

	command {
		set -euo pipefail
		gsutil -q -m cp -r ~{filterCount_directory} .
		gsutil -q -m cp -r ~{rawCount_directory} .
		gsutil -q -m cp ~{Script} .	
		mkdir ./~{Sample}_QCOut

		echo ${Sample}
		echo ${output_directory}
		echo "Start sctk-qc step"
		echo $(date +%T)
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
		-F ~{outputFormat} \
		-M ~{detectMitoLevel} \
		-E ~{mitoType}
		echo $(date +%T)
		mv ./*.html ./~{Sample}_QCOut
		gsutil -m cp -r ./~{Sample}_QCOut ~{output_directory}
		echo "Done sctk-qc step"
		echo $(date +%T)

	}
	output {
		File tmp = "./~{Sample}_QCOut/level3Meta.csv"
		#String sctkQC_output = "~{output_directory}"
	}
	runtime {
		# Use this container, pull from DockerHub   
		docker: sctk_docker
		cpu: numCore
		memory: memory
		disks: "local-disk ~{disk_space} HDD"
		bootDiskSizeGb: 50
		preemptible: preemptible
	} 
}