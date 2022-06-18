package decompress.entrance;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;

public class Merge124 {
    public static void main(String[] args) throws IOException {

        String mergeDir = args[0]";
        String mergeOutputDir = args[1];
         mergeAll(mergeDir, mergeOutputDir);
    }

    public static void mergeAll(String mergeTempDir, String mergeFinalDir) throws IOException {
        System.out.println("The merge begins.");
        long startTime2 = System.currentTimeMillis();
        File mergeFile = new File(mergeTempDir);
        File[] inputs = mergeFile.listFiles();
        assert inputs != null;
        assert inputs.length%3 == 0;
        for(int i = 0; i < inputs.length; i = i + 3) {
            String input1 = inputs[i].getAbsolutePath();
            String input2 = inputs[i+1].getAbsolutePath();
            String input4 = inputs[i+2].getAbsolutePath();
            System.out.println("Merge " + input1 + " " + input2 + " " + input4 + " => " + mergeFinalDir);
            merge(input1, input2, input4, mergeFinalDir);
        }

        System.out.println("The merge time is" + (System.currentTimeMillis() - startTime2) / 1000 + "s.");


    }


    public static void mergeAllForSpark(String mergeTempDir, String mergeFinalDir) throws IOException {
        System.out.println("The merge begins.");
        long startTime2 = System.currentTimeMillis();

        File dirA = new File(mergeTempDir + File.separator + "a");
        File dirB = new File(mergeTempDir + File.separator + "b");

        File[] inputs1 = dirA.listFiles();
        Arrays.sort(inputs1, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
            }
        });
        File[] inputs2 = dirB.listFiles();
        Arrays.sort(inputs2, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
            }
        });
        assert inputs1 != null;
        assert inputs1.length%2 == 0;
        assert inputs1.length == inputs2.length*2;
        int count = 0;
        for(int i = 0; i < inputs1.length; i = i + 2) {
            String input1 = inputs1[i].getAbsolutePath();
            String input2 = inputs1[i+1].getAbsolutePath();
            String input4 = inputs2[count++].getAbsolutePath();
            merge(input1, input2, input4, mergeFinalDir);
        }

        System.out.println("The merge time is" + (System.currentTimeMillis() - startTime2) / 1000 + "s.");


    }

    public static void merge(String input1, String input2, String input4, String mergeOutputDir) throws IOException {


        BufferedReader[] br = new BufferedReader[3];
        br[0] = new BufferedReader(new FileReader(input1));
        br[1] = new BufferedReader(new FileReader(input2));
        br[2] = new BufferedReader(new FileReader(input4));

        String outputFileName = new File(input1).getName();
        outputFileName = outputFileName.substring(0, outputFileName.length()-2);
        File output = new File(mergeOutputDir + File.separator + outputFileName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
        String line1 = null;
        String line2 = null;
        String line4 = null;
        while(     (line1=br[0].readLine()) != null && !"".equals(line1)
                && (line2=br[1].readLine()) != null && !"".equals(line2)
                && (line4=br[2].readLine()) != null && !"".equals(line4) ) {

            bw.write(line1);
            bw.newLine();
            bw.write(line2);
            bw.newLine();
            bw.write("+");
            bw.write(line1.substring(1));
            bw.newLine();
            bw.write(line4);
            bw.newLine();
        }

        bw.flush();
        bw.close();
        br[0].close();
        br[1].close();
        br[2].close();
    }

    public static int getLineNum(File input) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(input));
        int count = 0;
        int count1 = 0;
        String line1 = null;
        while((line1=br.readLine()) != null ) {
            count++;
            if( "".equals(line1)) {
                count1++;
            }
        }
        br.close();
        return count;
    }
}
