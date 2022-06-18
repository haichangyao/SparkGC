package decompress.util;

import java.io.*;

public class MergeFile {


    public static void main(String[] args) throws IOException {

        String input = args[0];
        String output = args[1];
        merge(input, output);


    }

    public static void merge(String input, String output) throws IOException {
        File file = new File(input);
        File[] files = file.listFiles();

        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        String line = null;
        for(int i = 0; i < files.length; i++) {
            BufferedReader br = new BufferedReader(new FileReader(files[i]));

            while((line=br.readLine()) != null) {
                bw.write(line);
                bw.newLine();
            }

            br.close();

        }
        bw.flush();
        bw.close();




    }



}
