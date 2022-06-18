package compress.tar;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class Tar {

    private static void saveName(FileSystem fs,String namePath,String out) {
        try {
            String name = null;
            FileStatus[] fss = fs.listStatus(new Path(namePath));
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(out)));
            for (FileStatus f : fss) {
                name = f.getPath().getName()+"\n";
                bw.write(name);
            }
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void FSfetch(String inputfile, String outfile,String finalOut){
        Configuration conf = new Configuration();
        Path inpath = new Path(outfile);

        String tmp = "/temp/gene";

        try {

            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf);

            File dir = new File(tmp+"/out");
            File output = new File(finalOut);
            dir.mkdirs();
            deleteFile(dir);

            fs.moveToLocalFile(inpath,new Path(tmp));

            saveName(fs,inputfile,tmp+"/out/hdfs_name.txt");

            archive(dir,new File(tmp+"/out.tar"));
            if(!output.exists()) System.out.println(output.mkdir());

            callShell("./bsc e "+tmp+"/out.tar "+finalOut+"/out.bsc");

            deleteFile(new File(tmp));
            dir.delete();
            deleteFile(new File(tmp+"/out.tar"));


            fs.close();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static boolean deleteFile(File dirFile) {
        if (!dirFile.exists()) {
            return false;
        }
        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {
            for (File file : Objects.requireNonNull(dirFile.listFiles())) {
                deleteFile(file);
            }
        }
        return dirFile.delete();
    }

    public static void archive(File srcFile, File destFile) throws IOException {
        System.out.println("archive begins：" + srcFile + " => " + destFile);

        long start = System.currentTimeMillis();
        TarArchiveOutputStream taos = new TarArchiveOutputStream(
                new FileOutputStream(destFile));
        archive(srcFile, taos, "");
        taos.flush();
        taos.close();

        System.out.println("archive complete. Time：" + (System.currentTimeMillis() - start)/1000 + "s");
    }

    private static void archive(File srcFile, TarArchiveOutputStream taos,
                                String basePath) throws IOException {
        if (srcFile.isDirectory()) {
            archiveDir(srcFile, taos, basePath);
        } else {
            archiveFile(srcFile, taos, basePath);
        }
    }

    private static void archiveDir(File dir, TarArchiveOutputStream taos,
                                   String basePath) throws IOException {
        File[] files = dir.listFiles();
        if (files.length < 1) {
            TarArchiveEntry entry = new TarArchiveEntry(basePath
                    + dir.getName() + File.separator);

            taos.putArchiveEntry(entry);
            taos.closeArchiveEntry();
        }
        for (File file : files) {
            archive(file, taos, basePath + dir.getName() + File.separator);
        }
    }

    private static void archiveFile(File file, TarArchiveOutputStream taos,
                                    String dir) throws IOException {
 
        TarArchiveEntry entry = new TarArchiveEntry(dir + file.getName());

        entry.setSize(file.length());

        taos.putArchiveEntry(entry);

        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(
                file));
        int count;
        byte []data = new byte[1<<12];//buffer volume
        while ((count = bis.read(data, 0, 1<<12)) != -1) {
            taos.write(data, 0, count);
        }

        bis.close();

        taos.closeArchiveEntry();
    }

    public static void callShell(String shellString) {
        try {
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                System.out.println("call shell failed. error code is :" + exitValue);
            }
        } catch (Throwable e) {
            System.out.println("call shell failed. " + e);
        }
    }

    public static void bscCompress(String srcFileName, String bscFileName) {
        System.out.println("bsc compression begins：" + srcFileName + " => " + bscFileName);
        long start = System.currentTimeMillis();

        callShell("/opt/temp/libbsc-master/bsc e " + srcFileName + " " + bscFileName + " -b64 -e2");

        System.out.println("archive complete. Time：" + (System.currentTimeMillis() - start)/1000 + "s");
    }

    public static void main(String[] args) throws IOException {

        File file0 = new File(args[0]);
        File file1 = new File(args[1]);
        archive(file0, file1);

    }
}
