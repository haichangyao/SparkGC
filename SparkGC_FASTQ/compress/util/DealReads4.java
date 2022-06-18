package compress.util;

import compress.entities.qualityS.QualityScores;
import compress.util.quality_func.QualityCompressor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

//import static compress.util.quality_func.QualityCompressor.bws;

public class DealReads4 {

    public static File[] input;
    public static File[] getInput() {
        return input;
    }

    private static File output ;
    private static BufferedWriter[] bw;

    public static void setGlobal(String inputDirName, String outputDir) throws IOException {
        output = new File(outputDir);
        if(!output.exists()) {
            output.mkdir();
        }

        File tmp = new File(inputDirName);
        if(tmp.isDirectory()){
            input = tmp.listFiles();
        }

        bw = new BufferedWriter[input.length];
        for(int i = 0; i < input.length; i++) {
            String inputFileName = input[i].getAbsolutePath();
            bw[i] = new BufferedWriter(new FileWriter(output + File.separator
                    + i + "-" + "b" + "-" + inputFileName.substring(inputFileName.lastIndexOf(File.separator) + 1)
                    + ".compress"));
        }
    }


    private QualityScores qs;
    private QualityCompressor qc;

    public DealReads4(){
    }

    public void compressFourthLinesForOneFastqFile(File fp, int k) throws IOException {
        qs = new QualityScores();
        qc = new QualityCompressor(bw[k]);
        try {
            LineIterator iter = FileUtils.lineIterator(fp,"UTF-8");
            int i = 0;
            boolean call = false;
            String str;
            while (iter.hasNext()){
                if(i!=3){
                    iter.nextLine();
                    i++;
                    continue;
                }

                if(!call){
                    call = qc.qs_compress(iter.nextLine(), qs, k);
                }else if((str=iter.nextLine())!=null){
                    qc.dealing(str, qs);
                }
                i = 0;
            }

            for(int j = 0; j < 2; ++j) {
                if(qc.sb[j].size() > 0) {
                    qc.writeOneMyByteBufferToFile(qc.sb[j], (byte) j, qc.pre[j], qc.cur[j], qc.eline[j]);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public List<Tuple2<String, byte[]>> compressFourthLinesForOneFastqFileForSpark2(Iterator<Tuple2<LongWritable, Text>> lines) throws IOException {
        qs = new QualityScores();
        qc = new QualityCompressor(200);
        try {
            int i = 0;
            boolean call = false;
            String str;
            while (lines.hasNext()){
                if(i!=3){
                    lines.next();
                    i++;
                    continue;
                }

                if(!call){
                    call = qc.qs_compressForSpark(lines.next()._2().toString(), qs);
                }else if((str=lines.next()._2().toString())!=null){
                    qc.dealingForSpark(str, qs);
                }
                i = 0;
            }

            for(int j = 0; j < 2; ++j) {
                if(qc.sb[j].size() > 0) {
                    qc.saveOneMyByteBufferToOutList(qc.sb[j], (byte) j, qc.pre[j], qc.cur[j], qc.eline[j]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return qc.getOutList();
    }
}
