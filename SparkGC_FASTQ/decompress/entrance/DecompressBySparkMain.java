package decompress.entrance;

import compress.entities.qualityS.BlockBufInfo;
import compress.entities.qualityS.MyByteBuffer;
import compress.tar.Tar;
import compress.util.DealReads;
import compress.util.mykryo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static compress.Entrance.mkdirIfNotExist;
import static compress.util.DealReads.MAX_LINE_LEN;
import static compress.util.SparkApp.deleteNotUsedFile;
import static decompress.entrance.BaseAndMarkerDecompress.decompressFirstAndSecond;
import static decompress.entrance.Merge124.*;
import static decompress.entrance.QualityDecompress.unpack;

public class DecompressBySparkMain {

    public static int linesPerMap = 0;

    public static void main(String[] args) throws Exception {
        String refFileName = null;
        String compressedOutputFileName = null;
        String decompressedOutputDirName= null;
        if(args != null && args.length == 3) {
            refFileName = args[0];/
            compressedOutputFileName = args[1];
            decompressedOutputDirName= args[2];
            decompressBySpark(refFileName, compressedOutputFileName, decompressedOutputDirName);
        } else {
            throw new Exception("The number of parameters is not 3.");
        }


    }

    public static void getProperties(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        BaseAndMarkerDecompress.SECOND_MATCH_REF_NUM = Integer.parseInt(br.readLine()) + 1;
        linesPerMap = Integer.parseInt(br.readLine());
        br.close();
        System.out.println("The number of second-order references is" + BaseAndMarkerDecompress.SECOND_MATCH_REF_NUM);
        System.out.println("linesPerMap:" + linesPerMap);
    }

    public static void decompressBySpark(String refFileName, String compressedOutputFileName, String decompressedOutputDirName) throws Exception {


        System.out.println("Decompression begins.");
        long startTime = System.currentTimeMillis();

        mkdirIfNotExist(decompressedOutputDirName);
        tar.dearchiveAndBsc(compressedOutputFileName,
                decompressedOutputDirName+".target",
                decompressedOutputDirName);

        getProperties(new File(decompressedOutputDirName  + File.separator + "compress-output" +   File.separator + "properties.txt"));

        String tarCompressedDirNameA = decompressedOutputDirName + File.separator + "compress-output" + File.separator + "a";
        String tarCompressedDirNameB = decompressedOutputDirName + File.separator + "compress-output" + File.separator + "b";

        String mergeTempDir = decompressedOutputDirName + File.separator + "temp";//
        String mergeFinalDir = decompressedOutputDirName + File.separator + "final";

        assert new File(tarCompressedDirNameA).exists();
        assert new File(tarCompressedDirNameB).exists();
        mkdirIfNotExist(mergeTempDir);
        mkdirIfNotExist(mergeFinalDir);

        File dirA = new File(mergeTempDir + File.separator + "a");
        mkdirIfNotExist(dirA.getAbsolutePath());
        decompressFirstAndSecond(refFileName, tarCompressedDirNameA, dirA.getAbsolutePath());

        File dirB = new File(mergeTempDir + File.separator + "b");
        decompressForthBySpark(tarCompressedDirNameB, dirB.getAbsolutePath());
        deleteNotUsedFile(dirB.getAbsolutePath());

        mergeAllForSpark(mergeTempDir, mergeFinalDir);

        mergeForNLine(mergeFinalDir);



        System.out.println("The decompression time is " + (System.currentTimeMillis() - startTime) / 1000 + "s.");
    }

    public static void decompressForthBySpark(String input, String output) throws IOException {


        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", mykryo.class.getName());
        sparkConf.setAppName("geneDecompress");
        sparkConf.setMaster("local[*]");
        sparkConf.set("spark.kryoserializer.buffer.max","2000").set("spark.driver.maxResultSize", "6g").set("spark.shuffle.sort.bypassMergeThreshold","20");
        sparkConf.set("spark.default.parallelism", "40").set("spark.shuffle.file.buffer","3000").set("spark.reducer.maxSizeInFlight", "1000")/*.set("spark.shuffle.io.maxRetries", "6")*/;
        sparkConf.set("spark.broadcast.blockSize", "256m");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.split.minsize","629145600");


        Path path2 = new Path(output);
        FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
        if(fs.exists(path2)){
            fs.delete(path2,true);
        }

        JavaRDD<Tuple2<String, byte[]>> objectJavaRDD = jsc.objectFile(input);
        objectJavaRDD.mapPartitions(datas -> {

            List<String> list1 = new ArrayList<>();
            List<String> list2 = new ArrayList<>();

            while (datas.hasNext()) {
                Tuple2<String, byte[]> next = datas.next();

                String key = next._1;
                String[] split = key.split(" ");
                assert split.length == 3;
                char score = split[0].charAt(0);
                int bucket = Integer.parseInt(split[1]);

                byte[] buff = next._2;

                BlockBufInfo bf = new BlockBufInfo();
                bf.bucket = (byte)bucket;
                bf.in = new MyByteBuffer();

                for(int j = 0; j < buff.length; j++) {
                    bf.in.put(buff[j]);
                }

                unpack(bf.in, score);

                char[] lineChar = new char[MAX_LINE_LEN];
                int len = 0;

                try{
                    int out = 0;
                    while((out=bf.in.get()) != -1) {
                        char ch = (char)out;
                        if(ch != '\n'){
                            lineChar[len++] = ch;
                        } else {
                            if(bf.bucket == 0) {
                                list1.add(new String(lineChar, 0, len));
                            } else {
                                list2.add(new String(lineChar, 0, len));
                            }
                            len = 0;
                        }
                    }
                } catch(ArrayIndexOutOfBoundsException e) {
                    System.out.println(bf.in.size());
                    System.out.println("lineChar.length=" + lineChar.length);
                    System.out.println("lineChar="+lineChar);
                }






            }

            List<String> list = new ArrayList<>(list1.size() + list2.size() + 10);
            int num1 = 0;
            int num2 = 0;
            while(num1 < list1.size()) {
                String temp = list1.get(num1++);
                if(!temp.equals("")) {
                    list.add(temp);
                } else {
                    list.add(list2.get(num2++));
                }
            }
            while(num2 < list2.size()) {
                list.add(list2.get(num2++));
            }

            return list.iterator();
        }).saveAsTextFile(path2.toString());


        deleteNotUsedFile(output);

        jsc.close();


    }

    public static void mergeForNLine(String mergeFinalDir) throws IOException {
        int NLineNum = linesPerMap;


        File file = new File(mergeFinalDir);
        File[] files = file.listFiles();

        int fileCount = 0;
        BufferedWriter bw = null;
        for(int i = 0; i < files.length; ) {
            bw = new BufferedWriter(new FileWriter(mergeFinalDir+File.separator+"final-" + fileCount++));
            while(i < files.length && getLineNum(files[i]) == NLineNum) {
                appendFileToFile(new BufferedReader(new FileReader(files[i])), bw);
                if(files[i].exists()) {
                    files[i].delete();
                }
                i++;
            }
            if(i < files.length) {
                appendFileToFile(new BufferedReader(new FileReader(files[i])), bw);
                if(files[i].exists()) {
                    files[i].delete();
                }
                i++;
            }
            bw.flush();
            bw.close();
        }
    }

    public static void appendFileToFile(BufferedReader br, BufferedWriter bw) throws IOException {
        String line  = null;
        while((line=br.readLine()) != null && !line.equals("")) {
            bw.write(line);
            bw.newLine();
        }
        bw.flush();
        br.close();
    }
}
