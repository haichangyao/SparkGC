package decompress.entrance;

import compress.entities.base_char.MatchEntry;
import compress.entities.base_char.Ref_base;
import compress.util.DealReads;
import compress.util.base_func.ComBase;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseAndMarkerDecompress {




    private static final List<String> seqPath = new ArrayList<>();
    private static int seqNumber;

    private static int linelen;
    private static int numOfBase;
    private static int MAX_CHA_NUM;

    private static String oneName;
    private static List<String> nameList;
    private static List<String> name1List;
    private static List<Integer> num0List;
    private static List<Integer> num1List;
    private static List<Integer> num2List;
    private static List<Integer> num3List;
    private static List<Integer> num4List;
    private static List<Integer> num5List;

    private static int[] spe_cha_pos;
    private static char[] spe_cha_ch;
    private static int spe_cha_len = 0;

    private static int[] nCha_begin;
    private static int[] nCha_length;
    private static int nCha_len = 0;
 
    private static int[] lineLens;
    private static int[] numOfSameLineLens;
    private static int lineLensSize = 0;

    private static char[] ref_code;
    private static int ref_code_len = 0;


    private static List <MatchEntry> matchResult;
    private static List <List <MatchEntry> > matchResult_vec = new ArrayList<>();

    private static StringBuilder seq_code;
    private static int seq_code_len = 0;

    private static StringBuilder sb = null;
    private static StringBuilder sb1 = null;

    public static int SECOND_MATCH_REF_NUM = 0;


    public static void initial(int numLine, int num) {
        linelen = numLine;
        numOfBase = num;
        MAX_CHA_NUM = numOfBase + 10;

        nameList = new ArrayList<>();
        num1List = new ArrayList<>();
        num2List = new ArrayList<>();

        spe_cha_pos = null;
        spe_cha_ch = null;
        spe_cha_len = 0;

        nCha_begin = null;
        nCha_length = null;
        nCha_len = 0;

        lineLens = null;
        numOfSameLineLens = null;
        lineLensSize = 0;

        matchResult = new ArrayList<>();




    }

    public static void main(String[] args) throws Exception {

        String refFileName;
        String tarCompressedDirName;
        String outputDirName;

        if(args != null && args.length == 3) {
            refFileName = args[0];
            tarCompressedDirName = args[1];
            outputDirName = args[2];
        }

        long startTime = System.currentTimeMillis();

        decompressFirstAndSecond(refFileName, tarCompressedDirName, outputDirName);

        System.out.println("Decompression completes. The decompression time is " + (System.currentTimeMillis() - startTime) / 1000 + "s.");

    }

    public static void decompressFirstAndSecond(String refFileName, String tarCompressedDirName, String mergeTempDir) throws Exception {
        System.out.println("The decompression of identifier and base sequence begins.");
        long startTime = System.currentTimeMillis();

        File outputDirFile = new File(mergeTempDir);
        if(!outputDirFile.exists()) {
            outputDirFile.mkdir();
        }

        refSequenceExtraction(refFileName);

        readFile(tarCompressedDirName);


        for(int i = 0; i < seqNumber; i++) {
            BufferedReader br = new BufferedReader(new FileReader(tarCompressedDirName + File.separator + seqPath.get(i)));
            String line = br.readLine();
            assert ">".equals(line.substring(0, 1));
            String[] fields = line.split("\\s+");
            int linelen = Integer.parseInt(fields[1]);
            int numOfBase = Integer.parseInt(fields[2]);
            initial(linelen, numOfBase);
            readOtherData2(br);

            readFirstLine(br);
            String fileName = mergeTempDir +
                    File.separator + seqPath.get(i) + "_";

            BufferedWriter bw1 = new BufferedWriter(new FileWriter(fileName + "1"));
            writeFirstLine(nameList, num0List, name1List, num1List, num2List,num3List,num4List,num5List, bw1);

            readFirstMatchResult(br, matchResult);

            if (i < SECOND_MATCH_REF_NUM && i < seqNumber) {
                matchResult_vec.add(matchResult);
            }

            recoverTarOnlyACGT(matchResult);
            System.out.println("matchResult.size()="+matchResult.size());

            BufferedWriter bw2 = new BufferedWriter(new FileWriter(fileName + "2"));
            writeSecondLine(bw2, -1, num3List);
        }

        System.out.println("The decompression time of identifier and base sequence is " + (System.currentTimeMillis() - startTime) / 1000 + "s.");


    }

    public static Ref_base refSequenceExtraction(String refFileName) throws IOException {
        String str;
        char[] cha;
        int _seq_code_len = 0;
       
        char temp_cha;
        File file = new File(refFileName);

        BufferedReader br = new BufferedReader(new FileReader(file));
        str = br.readLine();
        int k = 0;
        if(str.equals(">chr1")||str.equals(">chr2")||str.equals(">chr3")||str.equals(">chr4")
                ||str.equals(">chr5")||str.equals(">chr6")||str.equals(">chr7")||str.equals(">chr8")
                ||str.equals(">chr9")||str.equals(">chr10")||str.equals(">chr11")||str.equals(">chr12")
                ||str.equals(">chr13")||str.equals(">chrX")) k = 28;
        else k = 27;
        Ref_base ref = new Ref_base(file, k, true);

        while((str=br.readLine())!=null){
            cha = str.toCharArray();
            for (char a: cha) {
                temp_cha=a;
                if(Character.isLowerCase(temp_cha)){
                    temp_cha = Character.toUpperCase(temp_cha);
                }
                if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T'){
                    ref.set_Ref_code_byturn(temp_cha,_seq_code_len);
                    _seq_code_len++;
                }
            }
        }
        br.close();
        ref.set_Ref_code_len(_seq_code_len);

        ref_code = ref.getRef_code();
        ref_code_len = ref.getRef_code_len();

        System.out.println("The extraction of ["+refFileName+"] completes.");
        return ref;

    }

    private static void readFile(String filePath) throws Exception {
        File fp = new File(filePath);
        String[] names = fp.list();
        if (names == null)  {
            throw new Exception(filePath + "have no file.");
        }
        Arrays.sort(names);
        for (String a : names) {
            seqPath.add(a);
        }
        seqNumber = seqPath.size();
    }

    private static void readOtherData2(BufferedReader br) {

        try {
            String line = br.readLine();
            assert "*".equals(line);
            spe_cha_len = Integer.parseInt(br.readLine());
            if (spe_cha_len > 0) {
                spe_cha_pos = new int[spe_cha_len];
                spe_cha_ch = new char[spe_cha_len];
                readPositionRangeData2(br, spe_cha_len, BaseAndMarkerDecompress.spe_cha_pos, BaseAndMarkerDecompress.spe_cha_ch);
            }

            line = br.readLine();
            assert "*".equals(line);
            nCha_len = Integer.parseInt(br.readLine());
            if (nCha_len > 0) {
                nCha_begin = new int[nCha_len];
                nCha_length = new int[nCha_len];
                readPositionRangeData2(br, nCha_len, BaseAndMarkerDecompress.nCha_begin, BaseAndMarkerDecompress.nCha_length);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static  void readPositionRangeData2(BufferedReader br, int _vec_len, int[] _vec_begin, int[] _vec_length) {
        try {
            for (int i = 0; i < _vec_len; i++){
                String[] split = br.readLine().split("\\s+");
                _vec_begin[i] = Integer.parseInt(split[0]);
                _vec_length[i] = Integer.parseInt(split[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static  void readPositionRangeData2(BufferedReader br, int _vec_len, int[] _vec_begin, char[] _vec_length) {
        try {
            for (int i = 0; i < _vec_len; i++){
                String[] split = br.readLine().split("\\s+");
                _vec_begin[i] = Integer.parseInt(split[0]);
                _vec_length[i] = split[1].charAt(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void readFirstMatchResult(BufferedReader br, List<MatchEntry> _mr) {
        String str;
        String []temp_str ;
        StringBuilder _misStr = new StringBuilder();
        MatchEntry _me  = new MatchEntry();
        int _seq_id, _pos, _length, pre_seq_id = 0, pre_pos = 0;
        try {
            str = br.readLine();
            assert "*".equals(str);
            while ((str=br.readLine())!=null && !str.equals("*")) {
                temp_str = str.split("\\s+");
                if (temp_str.length==3) {
                    _seq_id = Integer.parseInt(temp_str[0]);
                    _pos = Integer.parseInt(temp_str[1]);
                    _length = Integer.parseInt(temp_str[2]);

                    _seq_id += pre_seq_id;
                    pre_seq_id = _seq_id;
                    _pos += pre_pos;
                    _length += 2;
                    pre_pos = _pos + _length;

                    getMatchResult(_seq_id, _pos, _length, _mr);
                } else if (temp_str.length==2) {
                    _pos = Integer.parseInt(temp_str[0]);
                    _length = Integer.parseInt(temp_str[1]);
                    _me.setPos(_pos);
                    _me.setLength(_length);
                    _me.setMisStr(_misStr.toString());
                    _mr.add(_me);
                    _me = new MatchEntry();
                    _misStr.delete(0,_misStr.length());
                } else if (temp_str.length==1) {
                    _misStr.append(temp_str[0]);
                }

            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getMatchResult(int _seq_id, int _pos, int _length, List <MatchEntry> _mr) {
        for (int i = 0; i < _length; i++)
            _mr.add(matchResult_vec.get(_seq_id).get(_pos++));
    }

    private static final char []integerEncoding = { 'A', 'C', 'G', 'T' };

    private static void recoverTarOnlyACGT(List <MatchEntry> _mr) {
        seq_code = new StringBuilder(MAX_CHA_NUM);
        seq_code_len = 0;

        int _pos, pre_pos=0, cur_pos,  _length, _seq_code_len=0, str_len;
        char[] _misStr;
        for (int i = 0; i < _mr.size();i++) {


            _pos = _mr.get(i).getPos();
            cur_pos = _pos + pre_pos;
            _length = _mr.get(i).getLength() + ComBase.MIN_REP_LEN;
            _misStr = _mr.get(i).getMisStr().toCharArray();

            str_len = _misStr.length;
            for (int k = 0; k < str_len; k++) {
                seq_code.append(integerEncoding[_misStr[k] - '0']);
                _seq_code_len++;
            }

            for (int m = cur_pos, n = 0; n < _length; n++, m++) {
                seq_code.append(ref_code[m]);
                _seq_code_len++;
            }

            pre_pos = cur_pos + _length;

        }
        seq_code_len = _seq_code_len;
    }


    private static void writeSecondLine(BufferedWriter bw, int _seqnum, List<Integer> num3List) throws IOException {
        int prePos = 0;
        if(spe_cha_len > 0) {
            sb = new StringBuilder(MAX_CHA_NUM);
            for(int i = 0; i < spe_cha_len; i++) {
                sb.append(seq_code, prePos, spe_cha_pos[i]);
                sb.append(spe_cha_ch[i]);
                prePos += spe_cha_pos[i];
            }
            seq_code = null;
        } else if (spe_cha_len == 0){
            sb = seq_code;
        }

        spe_cha_pos = null;
        spe_cha_ch = null;
        sb1 = new StringBuilder(MAX_CHA_NUM);
        prePos = 0;
        for(int i = 0; i < nCha_len; i++) {
            sb1.append(sb, prePos, prePos + nCha_begin[i]);
            for(int k = 0; k < nCha_length[i]; k++) {
                sb1.append('N');
            }


            prePos += nCha_begin[i];
        }
        sb = null;
        nCha_begin = null;
        nCha_length = null;
        for(int i = 0; i < num3List.size(); i++) {
            bw.write(sb1.substring(prePos, prePos+num3List.get(i)));
            bw.newLine();
            prePos += num3List.get(i);
        }

        sb1 = null;
        numOfSameLineLens = null;
        lineLens = null;

        bw.flush();
        bw.close();
    }

    private static void readFirstLine(BufferedReader br) throws IOException {
        //name
        String line = br.readLine();
        assert "*".equals(line);
        int num = Integer.parseInt(br.readLine());
        nameList = new ArrayList<>(linelen/4);
        for(int i = 0; i < num; i++) {
            String l = br.readLine();
            String[] split = l.split("\\s+");
            int len = Integer.parseInt(split[1]);
            for(int k = 0; k < len; k++) {
                nameList.add(split[0]);
            }
        }

        //num3
        line = br.readLine();
        assert "*".equals(line);
        num = Integer.parseInt(br.readLine());
        num3List = new ArrayList<>(linelen/4);
        int preNum3 = 0, curNum3 = 0;
        for(int i = 0; i < num; i++) {
            int deltaNum = Integer.parseInt(br.readLine());
            curNum3 = preNum3 + deltaNum;
            num3List.add(curNum3);
            preNum3 = curNum3;
        }

        //num4
        line = br.readLine();
        assert "*".equals(line);
        int num4 = Integer.parseInt(br.readLine());
        num4List = new ArrayList<>(linelen/4);
        int preNum4 = 0, curNum4 = 0;
        for(int i = 0; i < num4; i++) {
            int deltaNum = Integer.parseInt(br.readLine());
            curNum4 = preNum4 + deltaNum;
            num4List.add(curNum4);
            preNum4 = curNum4;
        }
    }


    private static void writeFirstLine(List<String> nameList, List<Integer> num0List, List<String> name1List,
                                       List<Integer> num1List, List<Integer> num2List, List<Integer> num3List,
                                       List<Integer> num4List, List<Integer> num5List,
                                       BufferedWriter bw) throws IOException {

        for(int i = 0; i < nameList.size(); i++) {
            bw.write(nameList.get(i));
            bw.write(".");

            bw.write(String.valueOf(num4List.get(i)));
            bw.write(".");

            bw.write("1");
            bw.write(" ");

            bw.write(String.valueOf(num4List.get(i)));
            bw.write(" ");
            bw.write("length");
            bw.write("=");
            bw.write(String.valueOf(num3List.get(i)));
            bw.newLine();
        }
        bw.flush();
        bw.close();



    }

}
