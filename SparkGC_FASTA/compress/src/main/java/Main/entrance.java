package Main;

import Service.App;
import scala.tools.nsc.Global;

import java.io.IOException;

public class entrance {

    public static void main(String[] args) throws IOException {

        String ref = args[0];//reference sequence
        String target = args[1];//to-be-compressed directory
        String hdfsOut = "/gene/out";//immediate directory in HDFS
        App.compress(ref,target,hdfsOut);

        tar t = new tar();
        String localOut = args[2];//output path
        t.FSfetch(target,hdfsOut,localOut);
    }

}
