package decompress.entrance;

import com.ning.compress.BufferRecycler;
import compress.entities.qualityS.BlockBufInfo;
import compress.entities.qualityS.MyByteBuffer;
import compress.util.quality_func.QualityCompressor;

import java.io.*;

public class QualityDecompress {

    private static File[] inputFile;
    private static char score;

    public static void main(String[] args) throws Exception {
        String tarCompressedDirName = args[0];
        String outputDirName = args[1];

        long startTime = System.currentTimeMillis();

        decompressFourth(tarCompressedDirName, outputDirName);

        System.out.println("Quality Decompression completes. The decompression time is" + (System.currentTimeMillis() - startTime) / 1000 + "s.");


    }

    public static void decompressFourth(String tarCompressedDirName, String mergeTempDir) throws Exception {
        System.out.println("Quality decompression begins.");

        long startTime1 = System.currentTimeMillis();

        File compressedDir = new File(tarCompressedDirName);
        inputFile = compressedDir.listFiles();

        File outputDir = new File(mergeTempDir);
        if(!outputDir.exists()) {
            outputDir.mkdir();
        }



        BufferedReader br = null;
        BufferedWriter bw0 = null;
        BufferedWriter bw1 = null;
        BufferedReader br0 = null;
        BufferedReader br1 = null;

        BufferedWriter bw = null;

        for(int i = 0; i < inputFile.length; i++) {
            br = new BufferedReader(new FileReader(inputFile[i]));
            String line = br.readLine();
            String[] split = line.split(" ");

            String[] temp = split[0].split("/|\\\\");
            String fastqFileName = temp[temp.length-1];
            File file0 = new File(mergeTempDir + File.separator + fastqFileName + "4-0");
            File file1 = new File(mergeTempDir + File.separator + fastqFileName + "4-1");
            File file = new File(mergeTempDir +  File.separator + fastqFileName + "4");
            bw0 = new BufferedWriter(new FileWriter(file0));
            bw1 = new BufferedWriter(new FileWriter(file1));
            bw = new BufferedWriter(new FileWriter(file));

            int fileIndex = Integer.parseInt(split[1]);
            assert fileIndex == i;
            score = split[2].charAt(0);//score
            assert split[2].toCharArray().length == 1;
            distinguishTwoClassBuffer(br, bw0, bw1);

            br0 = new BufferedReader(new FileReader(file0));
            br1 = new BufferedReader(new FileReader(file1));
            merge(br0, br1, bw);

            if(file0.exists()) {
                file0.delete();
            }
            if(file1.exists()) {
                file1.delete();
            }


            System.out.println("=================================");
        }

        System.out.println("The decompression time of quality is" + (System.currentTimeMillis() - startTime1) / 1000 + "s.");

    }

    public static void distinguishTwoClassBuffer(BufferedReader br, BufferedWriter bw0, BufferedWriter bw1) throws Exception {
        String line = null;
        while(true) {
            line = br.readLine();
            if(line == null || "".equals(line)) {
                break;
            }
            BlockBufInfo bf = new BlockBufInfo();
            String[] splits = line.split(" ");
            bf.bucket = (byte) Integer.parseInt(splits[0]);
            int numOfBytes = Integer.parseInt(splits[1]);
            bf.in = new MyByteBuffer();
            for(int j = 0; j < numOfBytes; j++) {
                int out = br.read();
                bf.in.put(out);
            }
            unpack(bf.in, score);
            int out = 0;
            while((out=bf.in.get()) != -1) {
                if (bf.bucket == 0) {
                    bw0.write(out);
                } else if (bf.bucket == 1) {
                    bw1.write(out);
                }
            }
            br.readLine();
        }
        bw0.flush();
        bw0.close();
        bw1.flush();
        bw1.close();
        br.close();
    }

    public static void merge(BufferedReader br0, BufferedReader br1, BufferedWriter bw) throws IOException {

        String line0 = null;
        String line1 = null;

        while( (line0=br0.readLine()) != null ) {
            if(!"".equals(line0)) {
                bw.write(line0);
                bw.newLine();
            } else if((line1=br1.readLine())!=null && !line1.equals("")){

                bw.write(line1);
                bw.newLine();
            }
        }
        bw.flush();
        bw.close();
        br0.close();
        br1.close();
    }

    public static void unpack(MyByteBuffer in, char score) throws Exception {
        MyByteBuffer out = new MyByteBuffer();

        int l2 = Math.max(score-7, 33), l3 = l2 + 4;
        for (int i = 0, c = 0; (c = in.get()) != -1;) {
            if (c == 0) {
                out.put(10);
                i = 0;
                continue;
            }
            else if (c >= 201) {
                while (c-->200) {
                    ++i;
                    out.put(score);
                }
            }
            else if (c >= 137 && c <= 200) {
                c -= 137;
                out.put((c & 3) + l3);
                out.put(((c >> 2) & 3) + l3);
                out.put(((c >> 4) & 3) + l3);
                i += 3;
            }
            else if (c >= 73 && c <= 136) {
                c -= 73;
                out.put((c & 7) + l2);
                out.put(((c >> 3) & 7) + l2);
                i += 2;
            }
            else if (c >= 1 && c <= 72) {
                out.put(c + 32);
                ++i;
            }
        }
        out.swap(in);
    }

    public static  void deleteCannotViewChar(String file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        BufferedWriter bw = new BufferedWriter(new FileWriter(file+".delete"));
        BufferedWriter bwDelete = new BufferedWriter(new FileWriter(file+".delete1"));
        int temp = 0;
        while(true) {
            temp = br.read();
            if(temp == -1) {
                break;
            }
            if(temp >= 32 && temp < 127 || temp == 10 || temp == 13 ){
                bw.write(temp);
            } else {
                bwDelete.write(String.valueOf(temp));
                bwDelete.newLine();
            }
            //(temp >=0 && temp <=31) || temp >=127

            
        }
        br.close();
        bw.flush();
        bw.close();
        bwDelete.flush();
        bwDelete.close();

    }
}
