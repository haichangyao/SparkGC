package compress.util;


import compress.entities.base_char.MatchEntry;
import compress.entities.base_char.Bases_Seq;
import compress.entities.base_char.Ref_base;
import compress.util.base_func.ComBase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.io.*;
import java.util.*;

public class DealReads {

    private Ref_base rb;

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
             + i + "-" + "a" + "-" + inputFileName.substring(inputFileName.lastIndexOf(File.separator) + 1)
             + ".compress"));
        }
    }

    private int linelen = 0;
    public void addLineLen() {
        linelen++;
    }


    private String preName = "";
    private int preNameNum = 0;

    private String preName1 = "";
    private int preNameNum1 = 0;

    private int baseNum0 = 0;
    private int preNum0 = 0;
    private int preNum0ChaFenNum = 0;

    private int preNum1 = 0;
    private int preNum1Num = 0;

    private int preNum2 = 0;
    private int preNum2Num = 0;

    private int preNum3 = 0;

    private int preNum4 = 0;

    private int preNum5 = 0;
    private int preNum5Num = 0;

    private List<String> nameList = new ArrayList<>();
    private List<String> name1List = new ArrayList<>();

    private List<String> num0List = new ArrayList<>();
    private List<String> num1List = new ArrayList<>();
    private List<String> num2List = new ArrayList<>();
    private List<Integer> num3List = new ArrayList<>();
    private List<Integer> num4List = new ArrayList<>();
    private List<String> num5List = new ArrayList<>();
    public List<String> outList = new ArrayList<>();
    public int numOfBase = 0;
    public int speCharPos = 0;


    private List<String> spe_cha_pos= new ArrayList<>();
    private List<String> spe_cha_ch= new ArrayList<>();
    private int spe_cha_len = 0;
    public void addSpe_cha_pos(int pos) {
        spe_cha_pos.add(String.valueOf(pos));
    }
    public void addSpe_cha_ch(int ch) {
        spe_cha_ch.add(String.valueOf(ch));
        spe_cha_len++;
    }

    public int n_letters_len = 0;
    public boolean n_flag = false;
    public List<String> nCha_begin = new ArrayList<>();
    private List<String> nCha_length = new ArrayList<>();
    public int nCha_len = 0;
    public void addnCha_begin(int begin) {
        nCha_begin.add(String.valueOf(begin));
    }
    public void addnCha_length(int length) {
        nCha_length.add(String.valueOf(length));
        nCha_len++;
    }

    public int preLineLen = 0;
    public int numSameLineLen = 0;

    private List<String> lineLens = new ArrayList<>();
    private List<String> numOfSameLineLens = new ArrayList<>();
    private int lineLensSize = 0;
    public void setNewLineLen(int newLineLen) {
        lineLens.add(String.valueOf(newLineLen));
        lineLensSize++;
    }
    public void setPreNumOfSameLineLen(int numOfSameLineLen) {
        numOfSameLineLens.add(String.valueOf(numOfSameLineLen));

    }

    public int prePosForFirstMatch = 0;
    public List<MatchEntry> me_t = new ArrayList<>();

    private static final int MAX_BASE_NUM = 20100000;
    public static final int MAX_LINE_LEN = 20000;
    private Bases_Seq currentBaseFileBlock = new Bases_Seq();
    public static int PERCENT = -1;
    private static Map<Integer,List<MatchEntry>> matchList = new HashMap<>();
    private static Map<Integer,List<Integer>> seqLocVec = new HashMap<>();
    private static Map<Integer,int[]> seqBucketVec = new HashMap<>();

    public DealReads() {
        initial();
    }

    public void initial() {
    }

    public DealReads(Ref_base rb){
        this.rb = rb;
    }

    public void compressFirstAndSecondLinesForOneFastqFileBySpark(Iterator<String> lines) {

        int i = 0;
        while (lines.hasNext()) {
            linelen++;
            switch (i) {
                case 0:
                    firstLine(lines.next());
                    ++i;
                    break;
                case 1:
                    secondLine(lines.next());
                    ++i;
                    break;
                case 2:
                    lines.next();
                    ++i;
                    break;
                case 3:
                    lines.next();
                    i = 0;
                    break;
            }
        }

        saoWei();

        saveFlag(rb.getRefFile(), null, -1);
        System.out.println("the Flag of the FastQ file save complete");

        saveOneFastQOtherInfo(outList);
        System.out.println("the number of the first-order MatchEntry ：" + me_t.size());
        System.out.println("the auxiliary data of FASTQ file save complete, the first-order matching complete.");
        saveOneFastQIdentifierInfo(outList);
        System.out.println("the identifier save complete.");
        System.out.println("save the auxiliary data to the outList.");


    }


    public void compressFirstAndSecondLinesForOneFastqFileBySpark2(Iterator<Tuple2<LongWritable, Text>> lines) {
        System.out.println("modify_id sub-module");

        int i = 0;
        while (lines.hasNext()) {
            linelen++;
            switch (i) {
                case 0:
                    firstLine(lines.next()._2().toString());
                    ++i;
                    break;
                case 1:
                    secondLine(lines.next()._2().toString());
                    ++i;
                    break;
                case 2:
                    lines.next();
                    ++i;
                    break;
                case 3:
                    lines.next();
                    i = 0;
                    break;
            }
        }
        saoWei();

        saveFlag(rb.getRefFile(), null, -1);
        saveOneFastQOtherInfo(outList);
        saveOneFastQIdentifierInfo(outList);
}

    public void compressFirstAndSecondLinesForOneFastqFile(File fp, int ii) {
        try {
            LineIterator iter = FileUtils.lineIterator(fp,"UTF-8");
            int i = 0;
            while (iter.hasNext()){
                linelen++;
                switch (i){
                    case 0:
                        firstLine(iter.nextLine());
                        ++i;
                        break;
                    case 1:
                        secondLine(iter.nextLine());
                        ++i;
                        break;
                    case 2:
                        iter.nextLine();
                        ++i;
                        break;
                    case 3:
                        iter.nextLine();
                        i=0;
                        break;
                }
            }
            saoWei();


            saveOneFastQOtherInfo(outList);
            saveOneFastQIdentifierInfo(outList);
            secondMatch(ii, outList);
            writeToFile(outList, bw[ii], rb.getRefFile(), fp, ii);
            } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void saveMatchEntryForDiff(List<MatchEntry> list , String fileName) throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        bw.write(list.size());
        bw.newLine();
        for(int i = 0; i < list.size(); i++) {
            bw.write(list.get(i).getPos() + " " + list.get(i).getLength() + " " + list.get(i).getMisStr());
            bw.newLine();
        }
        bw.flush();
        bw.close();

        System.exit(0);

    }
	
    public void saveOneFastQIdentifierInfo(List<String> outList) {
        outList.add("*");
        outList.add(String.valueOf(nameList.size()));
        outList.addAll(nameList);
        outList.add("*");
        outList.add(String.valueOf(num3List.size()));
        for(int i = 0; i < num3List.size(); i++) {
            outList.add(String.valueOf(num3List.get(i)));
        }

        outList.add("*");
        outList.add(String.valueOf(num4List.size()));
        for(int i = 0; i < num4List.size(); i++) {
            outList.add(String.valueOf(num4List.get(i)));
        }
        nameList = null;
        num2List = null;
        num3List = null;
        num4List = null;
        num5List = null;
    }

    public void writeToFile(List<String> outList, BufferedWriter bw, File ref, File tar, int ii){
        try {
            bw.write(">" + ref.getAbsolutePath() + " "  + linelen + " " + numOfBase);
            bw.newLine();
            for (String s : outList) {
                bw.write(s);
                bw.newLine();
            }
            bw.newLine();

            bw.flush();
            outList.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void saveFlag(File ref, File tar, int ii) {
        outList.add(">" + ref.getAbsolutePath() + " " + linelen + " " + numOfBase);
    }

    public void saoWei() {
        if (n_flag) {
            addnCha_length(n_letters_len);
        } else {
            addnCha_begin(n_letters_len);
            addnCha_length(0);
        }
        assert nCha_begin.size() == nCha_length.size();
        if(currentBaseFileBlock.getSeq_len() > 0) {
            List<MatchEntry> temp = ComBase.codeFirstMatch(currentBaseFileBlock,rb, this);
            me_t.addAll(temp);
        }
    }

    public void saveOneFastQOtherInfo(List<String> outList) {
        outList.add("*");
        outList.add(String.valueOf(spe_cha_len));
        for(int k = 0; k < spe_cha_len; k++) {
            outList.add(spe_cha_pos.get(k) + " " + spe_cha_ch.get(k));
        }
        outList.add("*");
        outList.add(String.valueOf(nCha_len));
        for(int k = 0; k < nCha_len; k++) {
            outList.add(nCha_begin.get(k) + " " + nCha_length.get(k));
        }
        spe_cha_pos = null;
        spe_cha_ch = null;
        nCha_begin = null;
        nCha_length = null;
        lineLens = null;
        numOfSameLineLens = null;

    }
    public void readFinal(){
    }

    public void firstLine(String str){
        String[] tmp = str.split("[. =]");
        String tname = tmp[0];
        if(tmp.length < 6)  {
            for(int i = 0; i < tmp.length; i++) {
                System.out.println("tmp["+i+"]="+tmp[i]);
            }
            throw new ArrayIndexOutOfBoundsException("The number of fields is less than 6.");
        }
        int num3 = Integer.parseInt(tmp[5]);
        int num4 = Integer.parseInt(tmp[1]);
        if (preName!=null && !preName.equals("")) {
            if(preName.equals(tname)) {
                preNameNum++;
            } else {
                nameList.add(preName + " " + preNameNum);
                preName = tname;
                preNameNum = 1;
            }
            num3List.add(num3- preNum3);
            preNum3 = num3;
            num4List.add(num4- preNum4);
            preNum4 = num4;
            return;
        }


        preName = tname;
        preNum3 = num3;
        preNum4 = num4;
        preNameNum = 1;
        num3List.add(num3);
        num4List.add(num4);
    }
    public void secondLine(String str){
        numOfBase += str.length();
        if(currentBaseFileBlock.getSeq_len()+str.length() < DealReads.MAX_BASE_NUM-MAX_LINE_LEN){
            ComBase.seqLines(str, currentBaseFileBlock, this);
            return;
        }

        if(currentBaseFileBlock.getSeq_len()+str.length() < DealReads.MAX_BASE_NUM) {
            ComBase.seqLines(str, currentBaseFileBlock, this);
        } else {
            throw new ArrayIndexOutOfBoundsException((currentBaseFileBlock.getSeq_len()+str.length())  + "exceeds" +DealReads.MAX_BASE_NUM);
        }

        List<MatchEntry> temp = ComBase.codeFirstMatch(currentBaseFileBlock, rb, this);
        me_t.addAll(temp);
        currentBaseFileBlock = new Bases_Seq();
    }

    private void secondMatch(int ii, List<String> outList){
	        if(ii <= PERCENT){
            ComBase.matchResultHashConstruct(me_t, seqBucketVec, seqLocVec,ii);
            matchList.put(ii,me_t);
        }


        outList.add("*");
        if(ii==0) {
            for (MatchEntry matchEntry : me_t) {
                saveMatchEntry(matchEntry, outList);
            }
        }else {
            ComBase.codeSecondMatch(me_t, ii+1, seqBucketVec, seqLocVec, matchList, outList, PERCENT +1);
        }

        me_t = null;
    }


    public void saveMatchEntry(MatchEntry matchEntry, List<String> outList) {
        StringBuilder sbf = new StringBuilder();
        if(!matchEntry.getMisStr().isEmpty()){
            outList.add(matchEntry.getMisStr());
        }
        sbf.append(matchEntry.getPos()).append(' ').append(matchEntry.getLength());
        outList.add(sbf.toString());
    }

    private void runLengthCoding(char []vec , int length,File file) throws IOException{
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));

        List<Integer> code=new ArrayList<>();
        if (length > 0) {
            code.add((int) vec[0]);
            int cnt = 1;
            for (int i = 1; i < length; i++) {
                if (vec[i] - vec[i-1] == 0)
                    cnt++;
                else {
                    code.add(cnt);
                    code.add((int) vec[i]);
                    cnt = 1;
                }
            }
            code.add(cnt);

        }
        for (int c : code) {
            bw.write(c);
        }
        bw.newLine();
    }




}
