package decompress.entrance;

import compress.Entrance;
import compress.tar.Tar;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static compress.Entrance.mkdirIfNotExist;
import static decompress.entrance.BaseAndMarkerDecompress.SECOND_MATCH_REF_NUM;
import static decompress.entrance.BaseAndMarkerDecompress.decompressFirstAndSecond;
import static decompress.entrance.Merge124.mergeAll;
import static decompress.entrance.Merge124.mergeAllForSpark;
import static decompress.entrance.QualityDecompress.decompressFourth;

public class DecompressMain {


    public static String refFileName = Entrance.refFileName;
    public static String compressedOutputFileName = Entrance.outputDir0 + ".target.bsc";
    public static String decompressedOutputDirName;



    public static void main(String[] args) throws Exception {

        if(args != null && args.length == 3) {
            refFileName = args[0];
            compressedOutputFileName = args[1];
            decompressedOutputDirName = args[2];
        } else {
            throw new Exception("The number of parameters is not 3.");
        }


        System.out.println("Compression begins.");
        long startTime = System.currentTimeMillis();

        mkdirIfNotExist(decompressedOutputDirName);
        tar.dearchiveAndBsc(compressedOutputFileName,
                decompressedOutputDirName+".target",
                decompressedOutputDirName);

        getProperties(new File(decompressedOutputDirName + File.separator + "compress-output" + File.separator + "properties.txt"));

        String tarCompressedDirNameA = decompressedOutputDirName + File.separator + "compress-output" + File.separator + "a";
        String tarCompressedDirNameB = decompressedOutputDirName + File.separator + "compress-output" + File.separator + "b";

        String mergeTempDir = decompressedOutputDirName + File.separator + "temp";//
        String mergeFinalDir = decompressedOutputDirName + File.separator + "final";

        assert new File(tarCompressedDirNameA).exists();
        assert new File(tarCompressedDirNameB).exists();
        mkdirIfNotExist(mergeTempDir);
        mkdirIfNotExist(mergeFinalDir);

        decompressFirstAndSecond(refFileName, tarCompressedDirNameA, mergeTempDir + File.separator + "a");
        decompressFourth(tarCompressedDirNameB, mergeTempDir + File.separator + "b");
        mergeAllForSpark(mergeTempDir, mergeFinalDir);

        Tar.deleteFile(new File( decompressedOutputDirName + File.separator + "compress-output"));
        Tar.deleteFile(new File(mergeTempDir));


        System.out.println("The decompression time is " + (System.currentTimeMillis() - startTime) / 1000 + "s.");
    }

    public static void getProperties(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        BaseAndMarkerDecompress.SECOND_MATCH_REF_NUM = Integer.parseInt(br.readLine()) + 1;
        br.close();
        System.out.println("The number of second-order references is" + BaseAndMarkerDecompress.SECOND_MATCH_REF_NUM);
    }
}
