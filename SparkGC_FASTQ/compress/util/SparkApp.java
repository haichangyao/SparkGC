package compress.util;


import compress.entities.base_char.MatchEntry;
import compress.entities.base_char.Ref_base;
import compress.entities.qualityS.BlockBufInfo;
import compress.entities.qualityS.MyByteBuffer;
import compress.entities.qualityS.QualityScores;
import compress.util.base_func.ComBase;
import compress.util.quality_func.QualityCompressor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static compress.Entrance.archiveAndBsc;
import static compress.Entrance.mkdirIfNotExist;
import static compress.util.base_func.ComBase.matchResultHashConstructForSpark;
import static decompress.entrance.QualityDecompress.unpack;
import static org.apache.spark.api.java.StorageLevels.MEMORY_ONLY_SER;

    public static void main(String[] args) throws Exception {
        System.out.println("args.length:" + args.length);
        if(args.length == 6) {
            refFileName = args[0];
            tar_file = args[1];
            out_path = args[2];
            out_path_local = args[3];
            per = Integer.parseInt(args[4]);
            linesPerMap = Integer.parseInt(args[5]);
        }

        long start = System.currentTimeMillis();
        System.out.println("compression begins：reference file:[" + refFileName + "] target file：[" + tar_file + " ] HDFS directory of compressed files：[" + out_path + "] local directory of compressed files:[" + out_path_local);

        compressFirstAndSecondBySpark(refFileName, tar_file, out_path, per, linesPerMap);

        getFileToLocalAndBsc(out_path, out_path_local, per, linesPerMap);

        System.out.println("compression complete. Time：" + (System.currentTimeMillis() - start)/1000 + "s.");
        File finalFile = new File(out_path_local + File.separator + "compress-output.target.bsc");
        System.out.println("the size of the compressed file：" + finalFile.length()*1.0/1024/1024 + "MB");


    }

    public static Tuple2<List<String>,List<MatchEntry>> spark_reads(Iterator<String> lines, DealReads dealReads){
        dealReads.compressFirstAndSecondLinesForOneFastqFileBySpark(lines);

        return new Tuple2<>(dealReads.outList, dealReads.me_t);


    }

    public static Tuple2<List<String>,List<MatchEntry>> spark_reads_2( Iterator<Tuple2<LongWritable, Text>> lines, DealReads dealReads){

        dealReads.compressFirstAndSecondLinesForOneFastqFileBySpark2(lines);

        return new Tuple2<>(dealReads.outList, dealReads.me_t);


    }

    public static void compressFirstAndSecondBySpark(String refFileName, String tar_file, String out_path, int _per, int linesPerMap) throws IOException {

        long start = System.currentTimeMillis();
        System.out.println("FirstAndSecondBySpark：reference file:[" + refFileName + "] target file：[" + tar_file + " ] directory of compressed file：[" + out_path + File.separator +"a");
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", mykryo.class.getName());
        sparkConf.setAppName("geneCompress");
        sparkConf.set("spark.kryoserializer.buffer.max","2000").set("spark.driver.maxResultSize", "50g").set("spark.shuffle.sort.bypassMergeThreshold","20");
        sparkConf.set("spark.default.parallelism", "40").set("spark.shuffle.file.buffer","3000").set("spark.reducer.maxSizeInFlight", "1000");
        sparkConf.set("spark.broadcast.blockSize", "256m");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.hadoopConfiguration().set("mapreduce.input.lineinputformat.linespermap",String.valueOf(linesPerMap));

        FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
        Path path2 = new Path(out_path);
        if(!fs.exists(path2)){
            fs.mkdirs(path2);
        }
        Path path3 = new Path(out_path + File.separator +"a");
        if(fs.exists(path3)) {
            fs.delete(path3, true);
        }

        Ref_base ref = ComBase.createRefBroadcast(refFileName);
        final Broadcast<Ref_base> referenceTypeBroadcast = jsc.broadcast(ref);
        System.out.println("hash index building and broadcast for the first-order compression complete.");
        int per = _per;
        final Broadcast<Integer> percent = jsc.broadcast(per);
        System.out.println("broadcast for the second-order compression complete.");
        JavaPairRDD<LongWritable, Text> nLineInput = jsc.newAPIHadoopFile(tar_file, NLineInputFormat.class, LongWritable.class, Text.class, jsc.hadoopConfiguration());
        System.out.println("the partition number of nLineInput rdd = "+nLineInput.getNumPartitions());
        JavaRDD<Tuple2<List<String>, List<MatchEntry>>> firstMatch = nLineInput.mapPartitions(datas -> {
            List<Tuple2<List<String>, List<MatchEntry>>> list = new ArrayList<>();

            DealReads dealReads = new DealReads(referenceTypeBroadcast.value());
            list.add(spark_reads_2(datas, dealReads));
            return list.iterator();
        });
        JavaRDD<Tuple3<Integer,List<String>,List<MatchEntry>>> index_firstMatch = firstMatch.mapPartitionsWithIndex((id, v2)->{
            List<Tuple3<Integer,List<String>,List<MatchEntry>>> list = new ArrayList<>();

            Tuple3<Integer,List<String>,List<MatchEntry>> t3 = null;
            while(v2.hasNext()){
                Tuple2<List<String>,List<MatchEntry>> t2 = v2.next();
                t3 = new Tuple3<>(id, t2._1, t2._2);
            }
            list.add(t3);

            return list.iterator();
        },true).persist(MEMORY_ONLY_SER);
        System.out.println("index for the fastq files complete:index_firstMatch");

        JavaRDD<Tuple3<Map<Integer, List<MatchEntry>>, Map<Integer, int[]>, Map<Integer, List<Integer>>>> secondMatchRef2 = index_firstMatch.filter(s -> s._1() <= percent.getValue())
                .mapPartitions(datas -> {
                    List<Tuple4<Integer, List<MatchEntry>, int[], List<Integer>>> list = new ArrayList<>();

                    Tuple4<Integer, List<MatchEntry>, int[], List<Integer>> index_matchList_bucket_loc = null;
                    while (datas.hasNext()) {
                        Tuple3<Integer, List<String>, List<MatchEntry>> index_other_matchList = datas.next();
                        Integer index = index_other_matchList._1();
                        List<String> other = index_other_matchList._2();
                        List<MatchEntry> matchList = index_other_matchList._3();
                        index_matchList_bucket_loc = matchResultHashConstructForSpark(matchList, index);

                        list.add(index_matchList_bucket_loc);
                    }


                    return list.iterator();
                }).coalesce(1, true)
                .mapPartitions(datas -> {
                    List<Tuple3<Map<Integer, List<MatchEntry>>, Map<Integer, int[]>, Map<Integer, List<Integer>>>> list = new ArrayList<>();

                    Map<Integer, List<MatchEntry>> matchList = new HashMap<>();
                    Map<Integer, int[]> seqBucket = new HashMap<>();
                    Map<Integer, List<Integer>> seqLoc = new HashMap<>();

                    while (datas.hasNext()) {
                        Tuple4<Integer, List<MatchEntry>, int[], List<Integer>> index_matchList_bucket_loc = datas.next();
                        Integer index = index_matchList_bucket_loc._1();
                        List<MatchEntry> _matchList = index_matchList_bucket_loc._2();
                        int[] hashBucket = index_matchList_bucket_loc._3();
                        List<Integer> hashLoc = index_matchList_bucket_loc._4();


                        matchList.put(index, _matchList);
                        seqBucket.put(index, hashBucket);
                        seqLoc.put(index, hashLoc);
                    }

                    list.add(new Tuple3<>(matchList, seqBucket, seqLoc));

                    return list.iterator();

                });
        Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>> ref2 = secondMatchRef2.first();
        if(ref2 != null) {
            System.out.println("the number of ref2 =" + ref2._1().size());
        } else {
            System.out.println("the number of ref2 =0");
        }
        System.out.println("the hash index for the second-order matching pull to drive complete :first");
        final Broadcast<Tuple3<Map<Integer,List<MatchEntry>>,   Map<Integer,int[]>,   Map<Integer,List<Integer>>>> sec_broad = jsc.broadcast(ref2);
        System.out.println("the hash index building and broadcasting for the second-order compression complete.");


        index_firstMatch.mapPartitions(datas->{
            int pr = percent.getValue();
            List<String> outList = new ArrayList<>();

            Tuple3<Integer,List<String>,List<MatchEntry>> index_other_firstMatch;
            while (datas.hasNext() ) {
                index_other_firstMatch = datas.next();

                outList = index_other_firstMatch._2();

                outList.add("*");
                if(pr >= 0 && sec_broad.value() != null) {//pre>=0
                    if(index_other_firstMatch._1() == 0){
                        for (int i = 0; i < index_other_firstMatch._3().size(); i++) {
                            ComBase.saveMatchEntry(outList, index_other_firstMatch._3().get(i));
                        }
                    } else {
                        ComBase.codeSecondMatch(index_other_firstMatch._3(), index_other_firstMatch._1() + 1,
                                sec_broad.value()._2(), sec_broad.value()._3(), sec_broad.value()._1(),
                                outList, pr+1);
                    }
                } else {//pre<0
                    for (int i = 0; i < index_other_firstMatch._3().size(); i++) {
                        ComBase.saveMatchEntry(outList, index_other_firstMatch._3().get(i));
                    }
                }

            }


            return outList.iterator();
        }).saveAsTextFile(path3.toString());

        System.out.println("saveAsTextFile complete.");

        secondMatchRef2.unpersist();
        firstMatch.unpersist();

        System.out.println("The compression of FirstAndSecondBySpark completes. Time：" + (System.currentTimeMillis() - start)/1000 + "s.");



        compressForthBySpark(tar_file, out_path, jsc, nLineInput);
    }



    public static void compressForthBySpark( String tar_file, String out_path, JavaSparkContext jsc, JavaPairRDD<LongWritable, Text> nLineInput) throws IOException {

        long start = System.currentTimeMillis();
        System.out.println("Quality score compression begins： target directory：[" + tar_file + " ], compressed directory：[" + out_path+ File.separator + "b");

        FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
        Path path2 = new Path(out_path);
        if(!fs.exists(path2)){
            fs.mkdirs(path2);
        }
        Path path3 = new Path(out_path+ File.separator + "b");
        if(fs.exists(path3)){
            fs.delete(path3, true);
        }

        System.out.println("the partition number of nLineInput RDD ="+nLineInput.getNumPartitions());

        JavaRDD<Tuple2<String, byte[]>> tuple2JavaRDD = nLineInput.mapPartitions(datas -> {
            List<Tuple2<String, byte[]>> list = null;

            DealReads4 dealReads4 = new DealReads4();
            list = dealReads4.compressFourthLinesForOneFastqFileForSpark2(datas);

            return list.iterator();
        });

        System.out.println("The partition number of tuple2JavaRDD RDD ="+tuple2JavaRDD.getNumPartitions());

        tuple2JavaRDD.saveAsObjectFile(path3.toString());

        System.out.println("The compression of quality score completes. Time：" + (System.currentTimeMillis() - start)/1000 + "s.");
    }

    public static void getFileToLocalAndBsc(String hdfsDirName, String out_path_local, int pre, int linesPerMap) throws IOException, URISyntaxException {
        long start = System.currentTimeMillis();
        System.out.println("Dowload HDFS files to local： hdfs files：[" + hdfsDirName + " ] local directory：[" + out_path_local);

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(new URI("hdfs://amd001:8020"), conf);

        mkdirIfNotExist(out_path_local);
        out_path_local = out_path_local  + File.separator + "compress-output";
        mkdirIfNotExist(out_path_local);

        Path path1 = new Path(hdfsDirName + File.separator + "a");
        Path path2 = new Path(out_path_local);
        fs.copyToLocalFile(path1, path2);
        path1 = new Path(hdfsDirName + File.separator + "b");
        fs.copyToLocalFile(path1, path2);
        deleteNotUsedFile(out_path_local + File.separator + "a");
        deleteNotUsedFile(out_path_local + File.separator + "b");

        saveProperties(out_path_local, pre, linesPerMap);

        archiveAndBsc(out_path_local, out_path_local+".target", out_path_local+".target.bsc");

        System.out.println("Dowload HDFS file to local and BSC compression take ：" + (System.currentTimeMillis() - start)/1000 + "s.");
    }

    public static void saveProperties(String out_path_local, int pre, int linesPerMap) throws IOException {
        File properFile = new File(out_path_local + File.separator + "properties.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(properFile));
        bw.write(String.valueOf(pre));
        bw.newLine();
        bw.write(String.valueOf(linesPerMap));
        bw.newLine();
        bw.flush();
        bw.close();
    }

    public static void deleteNotUsedFile(String dirName) {

        File file = new File(dirName);
        File[] files = file.listFiles();

        for(File tmp : files) {
            if(tmp.getName().endsWith(".crc") || tmp.getName().endsWith("_SUCCESS")) {
                tmp.delete();
            }
        }

    }
}
