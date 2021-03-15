 ****************************************************************************                             
	                              SparkGC
  (Spark Based Genome Compression for Large Collections of Genomes)

     https://github.com/haichangyao/SparkGC

          Copyright (C) 2021                  
****************************************************************************

1. Introduction

   SparkGC is implemented with Java and can be run on Linux operating system.

****************************************************************************

2. Use

2.1 Usage

2.1.1 Compress

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
	
2.1.2  Decompress
    java -jar decompress.jar {reference-file} {compressed-file} {decompressed-file-directory}
    decompress.jar is the application file name; 
    {reference-file} is the reference file name; 
    {compressed-file} is the compressed file name; 
    {decompressed-file-directory} is the directory used to store the decompressed files


2.2 Output:
    1.compressed file named out.bsc in the directory specified in the command line
    2.decompressed file named *.fa in the decompressed directory specified in the command line

****************************************************************************

3. Example

3.1 You can download the compress.jar and decompress.jar packages and the test datasets at: https://github.com/haichangyao/SparkGC/test

3.2 compress and decompress hg17_chr22.fa and hg18_chr22.fa, using hg13_chr22.fa as reference. The reference file is stored in the local file system, e.g. /home/reference/chr22; the to-be-compressed files are stored in the HDFS, e.g. hdfs://master:9000/chr22/; the compressed file out.bsc is stored in the local file system, e.g. /home/compressed; the decompressed files are stored in the local file system, e.g. /home/decompressed/chr22

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


3.3 use the‘diff’command of Linux to check the difference between the original files and the decompressed files

***************************************************************************