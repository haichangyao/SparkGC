package entrance;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.*;
import java.util.Objects;

public class tar {

    private static final int BUFFER = 1024*4;

    public static void BSC(String input,String output) throws Exception {
        String tmp = "/temp/gene";
        File tp = new File(tmp);
        if(!tp.exists()) tp.mkdirs();
        callShell("./bsc d "+input+" "+tmp+"/out.tar");
        TarArchiveInputStream tais = new TarArchiveInputStream(new FileInputStream(tmp+"/out.tar"));
        dearchive(new File(output),tais);
    }

    //delete directory
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


    /**
     * file dearchive
     *
     * @param destFile
     *            target file
     * @param tais
     *            ZipInputStream
     * @throws Exception
     */
    private static void dearchive(File destFile, TarArchiveInputStream tais)
            throws Exception {

        TarArchiveEntry entry = null;
        while ((entry = tais.getNextTarEntry()) != null) {
            String dir = destFile.getPath() + File.separator + entry.getName();
            File dirFile = new File(dir);
            fileProber(dirFile);
            if (entry.isDirectory()) {
                dirFile.mkdirs();
            } else {
                dearchiveFile(dirFile, tais);
            }
        }
    }


    /**
     * file dearchive
     *
     * @param destFile
     *            target file
     * @param tais
     *            TarArchiveInputStream
     * @throws Exception
     */
    private static void dearchiveFile(File destFile, TarArchiveInputStream tais)
            throws Exception {

        BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(destFile));

        int count;
        byte data[] = new byte[BUFFER];
        while ((count = tais.read(data, 0, BUFFER)) != -1) {
            bos.write(data, 0, count);
        }

        bos.close();
    }

    /**
     * file prober
     *
     * <pre>
     * when parent directory does not exist, create the directory
     * </pre>
     *
     * @param dirFile
     */
    private static void fileProber(File dirFile) {

        File parentFile = dirFile.getParentFile();
        if (!parentFile.exists()) {
            // file parent directory recursively
            fileProber(parentFile);
            parentFile.mkdir();
        }
    }

    //shell
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
}
