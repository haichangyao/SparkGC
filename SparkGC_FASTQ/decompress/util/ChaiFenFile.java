package decompress.util;

import java.io.*;

import static decompress.entrance.Merge124.getLineNum;

public class ChaiFenFile {

    public static void main(String[] args) throws IOException {

        String input;
        String output;
        int numOfOneSplit = 4600000;
        if(args.length == 3) {
            input = args[0];
            output = args[1];
            numOfOneSplit = Integer.valueOf(args[2]);
        }


        File file = new File(input);
        File[] files = file.listFiles();
        for(int i = 0; i < files.length; i++) {
            chaiFen(files[i].getAbsolutePath(), output, numOfOneSplit);
        }
    }

    public static void chaiFen(String fileName, String outputDirName, int numOfOneSplit) throws IOException {

        File outputDir = new File(outputDirName);
        File file = new File(fileName);
        int lineNum = getLineNum(file);
        System.out.println("lineNum=" + lineNum);

        BufferedReader br = new BufferedReader(new FileReader(fileName));

        int count = 0;

        int fileCount = 0;

        BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir.getAbsolutePath() + File.separator + file.getName() +"_" + fileCount));

        int lineCount = 0;
        String line = null;
        while( (line=br.readLine()) != null ) {

            if(lineCount >= numOfOneSplit) {
                bw.flush();
                bw.close();
                fileCount++;
                bw = new BufferedWriter(new FileWriter(outputDir.getAbsolutePath() + File.separator + file.getName() +"_" + fileCount));
                lineCount = 0;
            }


            bw.write(line);
            bw.newLine();
            count += 1;
            for(int i = 0; i < 3; i++) {
                if((line=br.readLine()) != null) {
                    bw.write(line);
                    bw.newLine();
                    count += 1;
                }
            }


            lineCount += 4;


        }


        System.out.println("count=" + count);


        bw.flush();
        bw.close();



    }


}
