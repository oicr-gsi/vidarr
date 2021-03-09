--
-- PostgreSQL database dump
--

-- Dumped from database version 12.6 (Ubuntu 12.6-1.pgdg18.04+1)
-- Dumped by pg_dump version 12.6 (Ubuntu 12.6-1.pgdg18.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: workflow_version; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.workflow_version (id, created, hash_id, metadata, modified, name, parameters, version, workflow_definition) FROM stdin;
1	2021-03-03 17:51:06-05	faabc0e93a95c5ed706a2ead840c6569a21d387b7594d0fd727c97cbf3e08994	{"bcl2barcode.counts": "file"}	2021-03-03 17:51:06-05	bcl2barcode	{"bcl2barcode.lanes": {"is": "list", "inner": "integer"}, "bcl2barcode.basesMask": "string", "bcl2barcode.runDirectory": "directory", "bcl2barcode.countDualIndex.mem": {"is": "optional", "inner": "integer"}, "bcl2barcode.countDualIndex.bgzip": {"is": "optional", "inner": "string"}, "bcl2barcode.countDualIndex.cores": {"is": "optional", "inner": "integer"}, "bcl2barcode.countSingleIndex.mem": {"is": "optional", "inner": "integer"}, "bcl2barcode.outputFileNamePrefix": {"is": "optional", "inner": "string"}, "bcl2barcode.countDualIndex.modules": {"is": "optional", "inner": "string"}, "bcl2barcode.countDualIndex.timeout": {"is": "optional", "inner": "integer"}, "bcl2barcode.countSingleIndex.bgzip": {"is": "optional", "inner": "string"}, "bcl2barcode.countSingleIndex.cores": {"is": "optional", "inner": "integer"}, "bcl2barcode.generateIndexFastqs.mem": {"is": "optional", "inner": "integer"}, "bcl2barcode.countSingleIndex.modules": {"is": "optional", "inner": "string"}, "bcl2barcode.countSingleIndex.timeout": {"is": "optional", "inner": "integer"}, "bcl2barcode.generateIndexFastqs.modules": {"is": "optional", "inner": "string"}, "bcl2barcode.generateIndexFastqs.timeout": {"is": "optional", "inner": "integer"}, "bcl2barcode.generateIndexFastqs.bcl2fastq": {"is": "optional", "inner": "string"}}	1.0.1	1
\.


--
-- Data for Name: workflow_run; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.workflow_run (id, arguments, completed, created, engine_parameters, hash_id, input_file_ids, labels, last_accessed, metadata, modified, started, workflow_version_id) FROM stdin;
1	{"bcl2barcode.lanes": [3], "bcl2barcode.basesMask": "Y1N*,I8,I8,N*", "bcl2barcode.runDirectory": {"type": "EXTERNAL", "contents": {"externalIds": [{"id": "GSICAPBENCH210114_4496_3", "provider": "production-benchmarking-provenance"}], "configuration": "~/projects/vidarr"}}, "bcl2barcode.countDualIndex.mem": null, "bcl2barcode.countDualIndex.bgzip": null, "bcl2barcode.countDualIndex.cores": null, "bcl2barcode.countSingleIndex.mem": null, "bcl2barcode.outputFileNamePrefix": null, "bcl2barcode.countDualIndex.modules": null, "bcl2barcode.countDualIndex.timeout": 12, "bcl2barcode.countSingleIndex.bgzip": null, "bcl2barcode.countSingleIndex.cores": null, "bcl2barcode.generateIndexFastqs.mem": null, "bcl2barcode.countSingleIndex.modules": null, "bcl2barcode.countSingleIndex.timeout": 12, "bcl2barcode.generateIndexFastqs.modules": null, "bcl2barcode.generateIndexFastqs.timeout": 12, "bcl2barcode.generateIndexFastqs.bcl2fastq": null}	\N	2021-03-09 13:26:55-05	null	4fa185675f259d5912f9d339f342627fb5f765738acaebdd3e2b4d163992fef9	{}	{}	\N	{"bcl2barcode.counts": {"type": "ALL", "contents": [{"outputDirectory": "~/projects/vidarr/"}]}}	2021-03-09 13:26:55-05	\N	1
\.


--
-- Data for Name: active_operation; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.active_operation (id, attempt, debug_info, recovery_state, status, type, workflow_run_id, engine_phase) FROM stdin;
\.


--
-- Data for Name: active_workflow_run; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.active_workflow_run (id, attempt, cleanup_state, completed, consumable_resources, created, engine_phase, external_input_ids_handled, modified, preflight_okay, real_input, started, target, waiting_resource, workflow_run_url) FROM stdin;
1	0	\N	\N	{}	2021-03-09 13:26:55-05	0	f	2021-03-09 14:16:55-05	t	\N	\N	std	The maximum number of bcl2barcode workflows has been reached.	\N
\.


--
-- Data for Name: analysis; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.analysis (id, analysis_type, created, file_md5sum, file_metatype, file_path, file_size, hash_id, labels, modified, workflow_run_id) FROM stdin;
\.


--
-- Data for Name: external_id; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.external_id (id, workflow_run_id, external_id, provider, created, modified, requested) FROM stdin;
1	1	GSICAPBENCH210114_4496_3	production-benchmarking-provenance	2021-03-09 13:26:55-05	2021-03-09 13:26:55-05	f
\.


--
-- Data for Name: analysis_external_id; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.analysis_external_id (external_id_id, analysis_id) FROM stdin;
\.


--
-- Data for Name: external_id_version; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.external_id_version (id, created, external_id_id, key, requested, value) FROM stdin;
1	2021-03-09 13:26:55-05	1	pinery-hash-7	2021-03-09 13:27:29.163955-05	396cc13b824be29e1e62993bb7d9d2e3d3000f275ff0339d3953b825e03f054b
2	2021-03-09 13:26:55-05	1	shesmu-sha1	2021-03-09 13:27:29.163955-05	8C7C27CC57166189D0D73B0A116293F24FF0379F
\.


--
-- Data for Name: flyway_schema_history; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.flyway_schema_history (installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success) FROM stdin;
1	0001	initial	SQL	V0001__initial.sql	-1866043809	vidarr	2021-03-03 17:34:21.641367	193	t
2	0002	operation phase	SQL	V0002__operation_phase.sql	-503906903	vidarr	2021-03-03 17:34:21.845718	3	t
\.


--
-- Data for Name: workflow; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.workflow (id, created, is_active, labels, max_in_flight, modified, name) FROM stdin;
1	2021-03-03 17:42:39-05	t	null	0	2021-03-09 10:35:34-05	bcl2barcode
\.


--
-- Data for Name: workflow_definition; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.workflow_definition (id, created, hash_id, modified, workflow_file, workflow_language) FROM stdin;
1	2021-03-03 17:51:06-05	f078d01f654f4c14ea5332ceb960cf3078ff20cd7bb82d19f49d55e95810f1a9	2021-03-03 17:51:06-05	version 1.0\n\nworkflow bcl2barcode {\n  input {\n    String runDirectory\n    Array[Int] lanes\n    String basesMask\n    String? outputFileNamePrefix\n  }\n\n  call generateIndexFastqs {\n    input:\n      runDirectory = runDirectory,\n      lanes = lanes,\n      basesMask = basesMask\n  }\n  if(defined(generateIndexFastqs.index2)) {\n    call countDualIndex {\n      input:\n        index1 = generateIndexFastqs.index1,\n        index2 = select_first([generateIndexFastqs.index2]),\n        outputFileNamePrefix = outputFileNamePrefix\n    }\n  }\n  if(!defined(generateIndexFastqs.index2)){\n    call countSingleIndex {\n      input:\n        index1 = generateIndexFastqs.index1,\n        outputFileNamePrefix = outputFileNamePrefix\n    }\n  }\n\n  output {\n    File counts = select_first([countSingleIndex.counts, countDualIndex.counts])\n  }\n\n  parameter_meta {\n    runDirectory: "Illumina run directory (e.g. /path/to/191219_M00000_0001_000000000-ABCDE)."\n    lanes: "A single lane or a list of lanes for no lane splitting (merging lanes)."\n    basesMask: "The bases mask to produce the index reads (e.g. single 8bp index = \\"Y1N*,I8,N*\\", dual 8bp index = \\"Y1N*,I8,I8,N*\\")."\n    outputFileNamePrefix: "Output prefix to prefix output file names with."\n  }\n\n  meta {\n    author: "Michael Laszloffy"\n    email: "michael.laszloffy@oicr.on.ca"\n    description: "bcl2barcode produces index (barcode) counts for all reads in a lane or set of lanes."\n    dependencies: [\n      {\n        name: "bcl2fastq/2.20.0.422",\n        url: "https://support.illumina.com/sequencing/sequencing_software/bcl2fastq-conversion-software.html"\n      },\n      {\n        name: "htslib/1.9",\n        url: "https://github.com/samtools/htslib"\n      }\n    ]\n    output_meta: {\n      counts: "Gzipped and sorted index counts in csv format (count,index)."\n    }\n  }\n}\n\ntask generateIndexFastqs {\n  input {\n    String runDirectory # TODO: switch to "Directory" when Cromwell supports Directory symlink localization\n    Array[Int] lanes\n    String basesMask\n    String bcl2fastq = "bcl2fastq"\n    String modules = "bcl2fastq/2.20.0.422"\n    Int mem = 32\n    Int timeout = 6\n  }\n\n  String outputDirectory = "out"\n\n  command <<<\n    ~{bcl2fastq} \\\n    --runfolder-dir "~{runDirectory}" \\\n    --intensities-dir "~{runDirectory}/Data/Intensities/" \\\n    --processing-threads 8 \\\n    --output-dir "~{outputDirectory}" \\\n    --create-fastq-for-index-reads \\\n    --sample-sheet "/dev/null" \\\n    --tiles "s_[~{sep='' lanes}]" \\\n    --use-bases-mask "~{basesMask}" \\\n    --no-lane-splitting \\\n    --interop-dir "~{outputDirectory}/Interop"\n  >>>\n\n  output {\n    File index1 = "~{outputDirectory}/Undetermined_S0_I1_001.fastq.gz"\n    File? index2 = "~{outputDirectory}/Undetermined_S0_I2_001.fastq.gz"\n  }\n\n  runtime {\n    memory: "~{mem} GB"\n    modules: "~{modules}"\n    timeout: "~{timeout}"\n  }\n\n  parameter_meta {\n    runDirectory: "Illumina run directory (e.g. /path/to/191219_M00000_0001_000000000-ABCDE)."\n    lanes: "A single lane or a list of lanes for no lane splitting (merging lanes)."\n    basesMask: "The bases mask to produce the index reads (e.g. single 8bp index = \\"Y1N*,I8,N*\\", dual 8bp index = \\"Y1N*,I8,I8,N*\\")."\n    bcl2fastq: "bcl2fastq binary name or path to bcl2fastq."\n    modules: "Environment module name and version to load (space separated) before command execution."\n    mem: "Memory (in GB) to allocate to the job."\n    timeout: "Maximum amount of time (in hours) the task can run for."\n  }\n\n  meta {\n    output_meta: {\n      index1: "Index 1 fastq.gz.",\n      index2: "Index 2 fastq.gz (if \\"basesMask\\" has specified a second index)."\n    }\n  }\n}\n\n task countSingleIndex {\n  input {\n    File index1\n    String? outputFileNamePrefix\n    String bgzip = "bgzip"\n    String modules = "htslib/1.9"\n    Int mem = 16\n    Int cores = 16\n    Int timeout = 6\n  }\n\n  command <<<\n    ~{bgzip} -@ ~{cores} -cd ~{index1} | \\\n    awk 'NR%4==2' | \\\n    awk '{\n            counts[$0]++\n    }\n\n    END {\n            for (i in counts) {\n                    print counts[i] "," i\n            }\n    }' | \\\n    sort -nr | \\\n    gzip -n > "~{outputFileNamePrefix}counts.gz"\n  >>>\n\n  output {\n    File counts = "~{outputFileNamePrefix}counts.gz"\n  }\n\n  runtime {\n    memory: "~{mem} GB"\n    cpu: "~{cores}"\n    modules: "~{modules}"\n    timeout: "~{timeout}"\n  }\n\n  parameter_meta {\n    index1: "First index fastq.gz of a single index run to perform counting on."\n    outputFileNamePrefix: "Output prefix to prefix output file names with."\n    bgzip: "bgzip binary name or path to bgzip."\n    modules: "Environment module name and version to load (space separated) before command execution."\n    mem: "Memory (in GB) to allocate to the job."\n    cores: "The number of cores to allocate to the job."\n    timeout: "Maximum amount of time (in hours) the task can run for."\n  }\n\n  meta {\n    output_meta: {\n      counts: "Gzipped and sorted index counts in csv format (count,index)."\n    }\n  }\n}\n\n task countDualIndex {\n  input {\n    File index1\n    File index2\n    String? outputFileNamePrefix\n    String bgzip = "bgzip"\n    String modules = "htslib/1.9"\n    Int mem = 16\n    Int cores = 16\n    Int timeout = 6\n  }\n\n  command <<<\n    paste -d '-' \\\n    <(~{bgzip} -@ ~{ceil(cores/2)} -cd ~{index1} | awk 'NR%4==2') \\\n    <(~{bgzip} -@ ~{floor(cores/2)} -cd ~{index2} | awk 'NR%4==2') | \\\n    awk '{\n            counts[$0]++\n    }\n\n    END {\n            for (i in counts) {\n                    print counts[i] "," i\n            }\n    }' | \\\n    sort -nr | \\\n    gzip -n > "~{outputFileNamePrefix}counts.gz"\n  >>>\n\n  output {\n    File counts = "~{outputFileNamePrefix}counts.gz"\n  }\n\n  runtime {\n    memory: "~{mem} GB"\n    cpu: "~{cores}"\n    modules: "~{modules}"\n    timeout: "~{timeout}"\n  }\n\n  parameter_meta {\n    index1: "First index fastq.gz of a dual index run to perform counting on."\n    index2: "Second index fastq.gz of a dual index run to perform counting on."\n    outputFileNamePrefix: "Output prefix to prefix output file names with."\n    bgzip: "bgzip binary name or path to bgzip."\n    modules: "Environment module name and version to load (space separated) before command execution."\n    mem: "Memory (in GB) to allocate to the job."\n    cores: "The number of cores to allocate to the job."\n    timeout: "Maximum amount of time (in hours) the task can run for."\n  }\n\n  meta {\n    output_meta: {\n      counts: "Gzipped and sorted index counts in csv format (count,index)."\n    }\n  }\n}\n	WDL_1_0
2	2021-03-08 17:57:28-05	f078d01f654f4c14ea5332ceb960cf3078ff20cd7bb82d19f49d55e95810f1a9	2021-03-08 17:57:28-05	version 1.0\n\nworkflow bcl2barcode {\n  input {\n    String runDirectory\n    Array[Int] lanes\n    String basesMask\n    String? outputFileNamePrefix\n  }\n\n  call generateIndexFastqs {\n    input:\n      runDirectory = runDirectory,\n      lanes = lanes,\n      basesMask = basesMask\n  }\n  if(defined(generateIndexFastqs.index2)) {\n    call countDualIndex {\n      input:\n        index1 = generateIndexFastqs.index1,\n        index2 = select_first([generateIndexFastqs.index2]),\n        outputFileNamePrefix = outputFileNamePrefix\n    }\n  }\n  if(!defined(generateIndexFastqs.index2)){\n    call countSingleIndex {\n      input:\n        index1 = generateIndexFastqs.index1,\n        outputFileNamePrefix = outputFileNamePrefix\n    }\n  }\n\n  output {\n    File counts = select_first([countSingleIndex.counts, countDualIndex.counts])\n  }\n\n  parameter_meta {\n    runDirectory: "Illumina run directory (e.g. /path/to/191219_M00000_0001_000000000-ABCDE)."\n    lanes: "A single lane or a list of lanes for no lane splitting (merging lanes)."\n    basesMask: "The bases mask to produce the index reads (e.g. single 8bp index = \\"Y1N*,I8,N*\\", dual 8bp index = \\"Y1N*,I8,I8,N*\\")."\n    outputFileNamePrefix: "Output prefix to prefix output file names with."\n  }\n\n  meta {\n    author: "Michael Laszloffy"\n    email: "michael.laszloffy@oicr.on.ca"\n    description: "bcl2barcode produces index (barcode) counts for all reads in a lane or set of lanes."\n    dependencies: [\n      {\n        name: "bcl2fastq/2.20.0.422",\n        url: "https://support.illumina.com/sequencing/sequencing_software/bcl2fastq-conversion-software.html"\n      },\n      {\n        name: "htslib/1.9",\n        url: "https://github.com/samtools/htslib"\n      }\n    ]\n    output_meta: {\n      counts: "Gzipped and sorted index counts in csv format (count,index)."\n    }\n  }\n}\n\ntask generateIndexFastqs {\n  input {\n    String runDirectory # TODO: switch to "Directory" when Cromwell supports Directory symlink localization\n    Array[Int] lanes\n    String basesMask\n    String bcl2fastq = "bcl2fastq"\n    String modules = "bcl2fastq/2.20.0.422"\n    Int mem = 32\n    Int timeout = 6\n  }\n\n  String outputDirectory = "out"\n\n  command <<<\n    ~{bcl2fastq} \\\n    --runfolder-dir "~{runDirectory}" \\\n    --intensities-dir "~{runDirectory}/Data/Intensities/" \\\n    --processing-threads 8 \\\n    --output-dir "~{outputDirectory}" \\\n    --create-fastq-for-index-reads \\\n    --sample-sheet "/dev/null" \\\n    --tiles "s_[~{sep='' lanes}]" \\\n    --use-bases-mask "~{basesMask}" \\\n    --no-lane-splitting \\\n    --interop-dir "~{outputDirectory}/Interop"\n  >>>\n\n  output {\n    File index1 = "~{outputDirectory}/Undetermined_S0_I1_001.fastq.gz"\n    File? index2 = "~{outputDirectory}/Undetermined_S0_I2_001.fastq.gz"\n  }\n\n  runtime {\n    memory: "~{mem} GB"\n    modules: "~{modules}"\n    timeout: "~{timeout}"\n  }\n\n  parameter_meta {\n    runDirectory: "Illumina run directory (e.g. /path/to/191219_M00000_0001_000000000-ABCDE)."\n    lanes: "A single lane or a list of lanes for no lane splitting (merging lanes)."\n    basesMask: "The bases mask to produce the index reads (e.g. single 8bp index = \\"Y1N*,I8,N*\\", dual 8bp index = \\"Y1N*,I8,I8,N*\\")."\n    bcl2fastq: "bcl2fastq binary name or path to bcl2fastq."\n    modules: "Environment module name and version to load (space separated) before command execution."\n    mem: "Memory (in GB) to allocate to the job."\n    timeout: "Maximum amount of time (in hours) the task can run for."\n  }\n\n  meta {\n    output_meta: {\n      index1: "Index 1 fastq.gz.",\n      index2: "Index 2 fastq.gz (if \\"basesMask\\" has specified a second index)."\n    }\n  }\n}\n\n task countSingleIndex {\n  input {\n    File index1\n    String? outputFileNamePrefix\n    String bgzip = "bgzip"\n    String modules = "htslib/1.9"\n    Int mem = 16\n    Int cores = 16\n    Int timeout = 6\n  }\n\n  command <<<\n    ~{bgzip} -@ ~{cores} -cd ~{index1} | \\\n    awk 'NR%4==2' | \\\n    awk '{\n            counts[$0]++\n    }\n\n    END {\n            for (i in counts) {\n                    print counts[i] "," i\n            }\n    }' | \\\n    sort -nr | \\\n    gzip -n > "~{outputFileNamePrefix}counts.gz"\n  >>>\n\n  output {\n    File counts = "~{outputFileNamePrefix}counts.gz"\n  }\n\n  runtime {\n    memory: "~{mem} GB"\n    cpu: "~{cores}"\n    modules: "~{modules}"\n    timeout: "~{timeout}"\n  }\n\n  parameter_meta {\n    index1: "First index fastq.gz of a single index run to perform counting on."\n    outputFileNamePrefix: "Output prefix to prefix output file names with."\n    bgzip: "bgzip binary name or path to bgzip."\n    modules: "Environment module name and version to load (space separated) before command execution."\n    mem: "Memory (in GB) to allocate to the job."\n    cores: "The number of cores to allocate to the job."\n    timeout: "Maximum amount of time (in hours) the task can run for."\n  }\n\n  meta {\n    output_meta: {\n      counts: "Gzipped and sorted index counts in csv format (count,index)."\n    }\n  }\n}\n\n task countDualIndex {\n  input {\n    File index1\n    File index2\n    String? outputFileNamePrefix\n    String bgzip = "bgzip"\n    String modules = "htslib/1.9"\n    Int mem = 16\n    Int cores = 16\n    Int timeout = 6\n  }\n\n  command <<<\n    paste -d '-' \\\n    <(~{bgzip} -@ ~{ceil(cores/2)} -cd ~{index1} | awk 'NR%4==2') \\\n    <(~{bgzip} -@ ~{floor(cores/2)} -cd ~{index2} | awk 'NR%4==2') | \\\n    awk '{\n            counts[$0]++\n    }\n\n    END {\n            for (i in counts) {\n                    print counts[i] "," i\n            }\n    }' | \\\n    sort -nr | \\\n    gzip -n > "~{outputFileNamePrefix}counts.gz"\n  >>>\n\n  output {\n    File counts = "~{outputFileNamePrefix}counts.gz"\n  }\n\n  runtime {\n    memory: "~{mem} GB"\n    cpu: "~{cores}"\n    modules: "~{modules}"\n    timeout: "~{timeout}"\n  }\n\n  parameter_meta {\n    index1: "First index fastq.gz of a dual index run to perform counting on."\n    index2: "Second index fastq.gz of a dual index run to perform counting on."\n    outputFileNamePrefix: "Output prefix to prefix output file names with."\n    bgzip: "bgzip binary name or path to bgzip."\n    modules: "Environment module name and version to load (space separated) before command execution."\n    mem: "Memory (in GB) to allocate to the job."\n    cores: "The number of cores to allocate to the job."\n    timeout: "Maximum amount of time (in hours) the task can run for."\n  }\n\n  meta {\n    output_meta: {\n      counts: "Gzipped and sorted index counts in csv format (count,index)."\n    }\n  }\n}\n	WDL_1_0
\.


--
-- Data for Name: workflow_version_accessory; Type: TABLE DATA; Schema: public; Owner: vidarr
--

COPY public.workflow_version_accessory (workflow_version, workflow_definition, filename) FROM stdin;
\.


--
-- Name: active_operation_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.active_operation_id_seq', 1, false);


--
-- Name: active_operation_workflow_run_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.active_operation_workflow_run_id_seq', 1, false);


--
-- Name: active_workflow_run_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.active_workflow_run_id_seq', 1, false);


--
-- Name: analysis_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.analysis_id_seq', 1, false);


--
-- Name: external_id_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.external_id_id_seq', 1, true);


--
-- Name: external_id_version_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.external_id_version_id_seq', 2, true);


--
-- Name: workflow_definition_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.workflow_definition_id_seq', 2, true);


--
-- Name: workflow_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.workflow_id_seq', 6, true);


--
-- Name: workflow_run_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.workflow_run_id_seq', 1, true);


--
-- Name: workflow_version_id_seq; Type: SEQUENCE SET; Schema: public; Owner: vidarr
--

SELECT pg_catalog.setval('public.workflow_version_id_seq', 1, true);


--
-- PostgreSQL database dump complete
--

