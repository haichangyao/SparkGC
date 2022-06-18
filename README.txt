 ****************************************************************************                             
	                              SparkGC
  (Spark Based Genome Compression for Large Collections of Genomes)

     https://github.com/haichangyao/SparkGC

          Copyright (C) 2022                  
****************************************************************************

1. Introduction

   -SparkGC is implemented with Java and can be run on Linux operating system.

****************************************************************************
2. Install
   - install BSC，and configure environment variables
   - install JDK-1.8.0，Hadoop-3.1.3, and Spark-2.1.1
****************************************************************************
3. Use

3.1 Compress

3.1.1 FASTA

    $SPARK_HOME/bin/spark-submit \                  
       --master {master_url}\
       --class (main_class_name} \
       --num-executors {number_of_executors} \
       --driver-memory {maximum_memory} \
       --executor-memory {maximum_memory} \
       compress.jar {reference-file} {to-be-compressed-file-directory} {compressed-file-directory}
    
	$SPARK_HOME/bin/spark-submit: run the executor in spark_submit command;
	--master {master_url}： set the runnning environment of Spark; 
	--class (main_class_name}: set the main class name of the executors; 
	--num-executors {number_of_executors}: set the number of executors run the application; 
	--driver-memory {maximum_memory}: set the maximum memory can be allocated to the application in the driver node; 
	--executor-memory {maximum_memory}: set the maximum memory can be allocated to the application in the executors; 
	compress.jar: the application file name; 
    {reference-file} is the reference file name; 
    {to-be-compressed-file-directory} is the HDFS directory of the to-be-compressed files; 
    {compressed-file-directory} is the directory used to store the compressed file,the compressed file name is ‘out.bsc’

3.1.2 FASTQ
    $SPARK_HOME/bin/spark-submit \
       --master {master_url}\
       --class (main_class_name} \
       --num-executors {number_of_executors} \
       --driver-memory {maximum_memory} \
       --executor-memory {maximum_memory} \
       --executor-cores {number_of_cores} \
       JarName {reference-file} {to-be-compressed-file-directory} {hdfsDir}{localDir} -1 linespermap
	
    {hdfsDir} is the directory of compressed files in the HDFS; 
    {localDir} is the directory of compressed files in the local file system; 
    {linespermap} is the number of lines for each block

3.2  Decompress
    java -jar JarName {reference-file} {compressed-file} {decompressed-file-directory}
    JarName is the application file name; 
    {reference-file} is the reference file name; 
    {compressed-file} is the compressed file name; 
    {decompressed-file-directory} is the directory used to store the decompressed files


3.3 Output:
    1.compressed file named out.bsc in the directory specified in the command line
    2.decompressed file named *.fa/fq in the decompressed directory specified in the command line

****************************************************************************

4. Example

4.1 FASTA
compress and decompress hg17_chr22.fa and hg18_chr22.fa, using hg13_chr22.fa as reference. The reference file is stored in the local file system, e.g. /home/reference/chr22; the to-be-compressed files are stored in the HDFS, e.g. hdfs://master:9000/chr22/; the compressed file out.bsc is stored in the local file system, e.g. /home/compressed; the decompressed files are stored in the local file system, e.g. /home/decompressed/chr22

//Compress    
   $SPARK_HOME/bin/spark-submit \
       --master yarn \
       --deploy-mode client \
       --class Main.entrance \
       --num-executors 4 \
       --driver-memory 18g \
       --executor-memory 25000m \
       --executor-cores 8 \
       --conf spark.core.connection.ack.wait.timeout=300 \
       compress.jar /home/reference/chr22/hg13_chr22.fa hdfs://master:9000/chr22/ /home/compressed/

//Decompress
    java -jar decompress.jar /home/reference/chr22/hg13_chr22.fa out.bsc  /home/decompressed/chr22

4.2 FASTQ
compress and decompress SRR17714832.fastq, using hg13_chr22.fa as reference. The reference file is stored in the local file system, e.g. /home/reference/fastq; the to-be-compressed files are stored in the HDFS, e.g. hdfs://master:9000/fastq/; the compressed file out.bsc is stored in the local file system, e.g. /home/compressed; the decompressed files are stored in the local file system, e.g. /home/decompressed

//Compress    
   $SPARK_HOME/bin/spark-submit \
       --master yarn \
       --deploy-mode client \
       --class Main.entrance \
       --num-executors 4 \
       --driver-memory 18g \
       --executor-memory 25000m \
       --executor-cores 8 \
       --conf spark.core.connection.ack.wait.timeout=300 \
       SparkGC_FASTQ.jar /home/reference/fastq/hg13_chr22.fa hdfs://master:9000/fastq/SRR17714832.fastq hdfs://master:9000/compressed/ /home/compressed/

//Decompress
    java -jar SparkGC_FASTQ.jar /home/reference/fastq/hg13_chr22.fa out.bsc  /home/decompressed

4.3 use the‘diff’command of Linux to check the difference between the original files and the decompressed files

***************************************************************************