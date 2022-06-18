package compress.util.base_func;

import compress.entities.base_char.MatchEntry;
import compress.entities.base_char.Bases_Seq;
import compress.entities.base_char.Ref_base;
import compress.util.DealReads;
import scala.Tuple4;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Math.abs;
import static java.lang.Math.min;

public class ComBase {

    private static final int VEC_SIZE = 1 << 20;
    //    private static final int PERCENT = 15; //the percentage of compressed sequence number uses as reference
    private static final int kMerLen = 14; 
    private static final int kmer_bit_num = 2 * kMerLen; //bit numbers of k-mer
    private static final int hashTableLen = 1 << kmer_bit_num; // length of hash table//2 6843 5456
    //    private static int sec_seq_num = 120;
    private static int seqBucketLen = getNextPrime(1<<20);

    public static final int MIN_REP_LEN = 20;

    public static Ref_base createRefBroadcast(String filename) throws IOException {
        String str;
        char []cha;
        int _seq_code_len = 0, _ref_low_len = 1, letters_len = 0;//record lowercase from 1, diff_lowercase_loc[i]=0 means mismatching
        boolean flag = true;

        char temp_cha;
        File file = new File(filename);


        BufferedReader br = new BufferedReader(new FileReader(file));
        str = br.readLine();

        
        int k = 0;
        if(str.equals(">chr1")||str.equals(">chr2")||str.equals(">chr3")||str.equals(">chr4")
                ||str.equals(">chr5")||str.equals(">chr6")||str.equals(">chr7")||str.equals(">chr8")
                ||str.equals(">chr9")||str.equals(">chr10")||str.equals(">chr11")||str.equals(">chr12")
                ||str.equals(">chr13")||str.equals(">chrX")) k = 28;
        else k = 27;
        Ref_base ref = new Ref_base(file, k);


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
                letters_len++;
            }
        }
        br.close();
        ref.set_Ref_code_len(_seq_code_len);
        ref.set_Ref_low_len(_ref_low_len);


        kMerHashingConstruct(ref);
        System.out.println("reference sequence ["+filename+"] first-order hash index complete！");
        return ref;
    }

    private static int getNextPrime(int number) {
        int cur = number + 1;
        boolean prime = false;
        while (!prime)
        {
            prime = true;
            for (int i = 2; i < Math.sqrt(number) + 1; i++) {
                if (cur % i == 0) {
                    prime = false;
                    break;
                }
            }

            if (!prime) cur++;
        }
        return cur;
    }


    private static void kMerHashingConstruct(Ref_base ref){
        //initialize the point array
        for (int i = 0; i < hashTableLen; i++)
            ref.setrefBucket_byturn(-1,i);
        int value = 0;
        int step_len = ref.getRef_code_len() - kMerLen + 1;

        for (int k = kMerLen - 1; k >= 0; k--) {
            value <<= 2;
            value += integerCoding(ref.get_Ref_code_Byturn(k));
        }

        ref.setrefLoc_byturn(ref.getRefBucket_Byturn(value),0);
        ref.setrefBucket_byturn(0,value);

        int shift_bit_num = (kMerLen * 2 - 2);
        int one_sub_str = kMerLen - 1;

        for (int i = 1; i < step_len; i++) {
            value >>= 2;
            value += (integerCoding(ref.get_Ref_code_Byturn(i + one_sub_str))) << shift_bit_num;
            ref.setrefLoc_byturn(ref.getRefBucket_Byturn(value),i);  
            ref.setrefBucket_byturn(i,value);
        }
    }


    private static int integerCoding(char ch) { //encoding ACGT
        switch (ch) {
            case 'A': return 0;
            case 'C': return 1;
            case 'G': return 2;
            case 'T': return 3;
            default : return -1;
        }
    }

    public static void seqLines(String str, Bases_Seq c, DealReads dealReads/*, int n_letters_len, boolean n_flag*/){

        char temp_cha;
        char[] cha;      //the content of one line

        cha=str.toCharArray();

        if(cha.length != dealReads.preLineLen) {
            if(dealReads.numSameLineLen != 0) {
                dealReads.setPreNumOfSameLineLen(dealReads.numSameLineLen);
            }

            dealReads.setNewLineLen(cha.length);
            dealReads.numSameLineLen = 1;
            dealReads.preLineLen = cha.length;
        } else {
            dealReads.numSameLineLen++;
        }

        for (int i = 0; i < cha.length; i++){
            temp_cha = cha[i];

            if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T') {//ACGT
                dealReads.speCharPos++;

                c.addSeq_code(temp_cha);
            } else if (temp_cha != 'N') {

                dealReads.addSpe_cha_pos(dealReads.speCharPos);
                dealReads.addSpe_cha_ch(temp_cha - 'A');

                dealReads.speCharPos = 0;
            }


            if (!dealReads.n_flag) {
                if (temp_cha == 'N') {
                    dealReads.addnCha_begin(dealReads.n_letters_len);
                    dealReads.n_letters_len = 0;
                    dealReads.n_flag = true;
                }
            }
            else {
                if (temp_cha != 'N') {
                    dealReads.addnCha_length(dealReads.n_letters_len);
                    dealReads.n_letters_len = 0;
                    dealReads.n_flag = false;
                }
            }
            dealReads.n_letters_len++;
        }
    }


    private static List<Integer> runLengthCoding(int []vec , int length, int tolerance) {
        List<Integer> code=new ArrayList<>();
        if (length > 0) {
            code.add(vec[0]);
            int cnt = 1;
            for (int i = 1; i < length; i++) {
                if (vec[i] - vec[i-1] == tolerance)
                    cnt++;
                else {
                    code.add(cnt);
                    code.add(vec[i]);
                    cnt = 1;
                }
            }
            code.add(cnt);
        }
        return code;
    }


    public static List<MatchEntry> codeFirstMatch(Bases_Seq tar, Ref_base ref, DealReads dealReads) {
        int min_rep_len = MIN_REP_LEN;
        int step_len = tar.getSeq_len() - kMerLen + 1;
        int max_length, max_k;
        int i, id, k, ref_idx, tar_idx, length, cur_pos, tar_value;
        StringBuilder mismatched_str = new StringBuilder();
        List<MatchEntry> mr = new ArrayList<>();
        MatchEntry me ;


        for (i = 0; i < step_len; i++) {

            tar_value = 0;
            for (k = kMerLen - 1; k >= 0; k--) {
                tar_value <<= 2;
                tar_value += integerCoding(tar.getSeq_code_byturn(i+k));
            }

            id = ref.getRefBucket_Byturn(tar_value);

            if (id > -1) {//there is a same k-mer in ref_seq_code
                //search the longest match in the linked list
                max_length = -1;
                max_k = -1;//
                for (k = id; k != -1; k = ref.getrefLoc_byturn(k)) {
                    ref_idx = k + kMerLen;
                    tar_idx = i + kMerLen;
                    length = kMerLen;
                    while (ref_idx < ref.getRef_code_len() && tar_idx < tar.getSeq_len() &&
                            ref.get_Ref_code_Byturn(ref_idx++) == tar.getSeq_code_byturn(tar_idx++))
                        length++;

                    if (length >= min_rep_len && length > max_length) {
                        max_length = length;
                        max_k = k;
                    }
                }
                if (max_length > -1) {
                    me= new MatchEntry();
                    //then save matched information
                    cur_pos = max_k - dealReads.prePosForFirstMatch;      //delta-coding for cur_pos
                    me.setPos(cur_pos);//
                    me.setLength(max_length - min_rep_len);
                    me.setMisStr(mismatched_str.toString());
                    mr.add(me);

                    i += max_length;
                    dealReads.prePosForFirstMatch = max_k + max_length;
                    mismatched_str.delete(0,mismatched_str.length());
                    i--;
                    continue;
                }
            }

            mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;
        }

        if (i < tar.getSeq_len()) {
            for (; i < tar.getSeq_len(); i++)
                mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;
            me= new MatchEntry();
            me.setPos(0);
            me.setLength(-min_rep_len);                
            me.setMisStr(mismatched_str.toString());
            mr.add(me);
        }
        tar.setSeq_code(null);
        return mr;
    }

    private static int getHashValue(MatchEntry me) {
        int result = 0;
        for (int i = 0; i < me.getMisStr().length(); i++) {
            result += me.getMisStr().charAt(i) * 92083;
        }
        result += me.getPos() * 69061 + me.getLength() * 51787;
        result %= getNextPrime(1<<20);
        return result;
    }

    public static void matchResultHashConstruct(List<MatchEntry> matchResult, Map<Integer,int[]> seqBucketVec, Map<Integer,List<Integer>> seqLocVec, int Num) {
        int hashValue1, hashValue2, hashValue;
        List<Integer> seqLoc = new ArrayList<>(Math.max(VEC_SIZE, matchResult.size()+10));
        int []seqBucket = new int[seqBucketLen];
        for (int i = 0; i < seqBucketLen; i++) {
            seqBucket[i] = -1;
        }

        hashValue1 = getHashValue(matchResult.get(0)); 
        if (matchResult.size() < 2) {
            hashValue2 = 0;
        } else {
            hashValue2 = getHashValue(matchResult.get(1));
        }
        hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
        seqLoc.add(seqBucket[hashValue]);
        seqBucket[hashValue] = 0;

        for (int i = 1; i < matchResult.size() - 1; i++) {
            hashValue1 = hashValue2;
            hashValue2 = getHashValue(matchResult.get(i + 1));
            hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
            seqLoc.add(seqBucket[hashValue]);
            seqBucket[hashValue] = i;
        }
        seqLocVec.put(Num,seqLoc);
        seqBucketVec.put(Num,seqBucket);
    }

    public static Tuple4<Integer, List<MatchEntry>, int[], List<Integer>> matchResultHashConstructForSpark(List<MatchEntry> matchResult,  int Num) {
        int hashValue1, hashValue2, hashValue;
        List<Integer> seqLoc = new ArrayList<>(Math.max(VEC_SIZE, matchResult.size()+10));
        int []seqBucket = new int[seqBucketLen];
        for (int i = 0; i < seqBucketLen; i++) {
            seqBucket[i] = -1;
        }

        hashValue1 = getHashValue(matchResult.get(0));
        if (matchResult.size() < 2) {
            hashValue2 = 0;
        } else {
            hashValue2 = getHashValue(matchResult.get(1));
        }
        hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
        seqLoc.add(seqBucket[hashValue]);
        seqBucket[hashValue] = 0;

        for (int i = 1; i < matchResult.size() - 1; i++) {
            hashValue1 = hashValue2;
            hashValue2 = getHashValue(matchResult.get(i + 1));
            hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
            seqLoc.add(seqBucket[hashValue]);
            seqBucket[hashValue] = i;
        }


        return new Tuple4<>(Num, matchResult, seqBucket, seqLoc);
    }

    private static int getMatchLength(List <MatchEntry> ref_me, int ref_idx, List <MatchEntry> tar_me, int tar_idx) {
        int length = 0;
        while (ref_idx < ref_me.size() && tar_idx < tar_me.size() && compareMatchEntry(ref_me.get(ref_idx++), tar_me.get(tar_idx++)))
            length++;
        return length;
    }

    private static Boolean compareMatchEntry(MatchEntry ref, MatchEntry tar) {
        return  ref.getPos() == tar.getPos() && ref.getLength() == tar.getLength() && ref.getMisStr().equals(tar.getMisStr());
    }


    public static void saveMatchEntry(List<String> list, MatchEntry matchEntry) {
        StringBuilder sbf = new StringBuilder();
        if(!matchEntry.getMisStr().isEmpty()){
            list.add(matchEntry.getMisStr());
        }
        sbf.append(matchEntry.getPos()).append(' ').append(matchEntry.getLength());
        list.add(sbf.toString());
    }


    public static void codeSecondMatch(List<MatchEntry> _mr,     int seqNum,
            Map<Integer, int[]> seqBucket_vec,     Map<Integer,List<Integer>> seqLoc_vec ,
            Map<Integer,List<MatchEntry>> matchResult_vec,
            List<String> list,      int percent) {


        int hashValue;
        int pre_seq_id=1;
        int max_pos=0, pre_pos=0, delta_pos, length, max_length, delta_length, seq_id=0, delta_seq_id;
        int id, pos, secondMatchTotalLength=0;
        int i;
        StringBuilder sbt = new StringBuilder();
        ArrayList<MatchEntry> misMatchEntry = new ArrayList<>();
        for (i = 0; i < _mr.size()-1; i++) {
            if(_mr.size()<2) hashValue = abs(getHashValue(_mr.get(i))) % seqBucketLen;
            else hashValue = abs(getHashValue(_mr.get(i)) + getHashValue(_mr.get(i+1))) % seqBucketLen;
            max_length = 0;
            for (int m = 0; m < min( seqNum-1, percent); m++) {
                id = seqBucket_vec.get(m)[hashValue];
                if (id!=-1) {
                    for (pos = id; pos!=-1; pos = seqLoc_vec.get(m).get(pos)) {
                        length = getMatchLength(matchResult_vec.get(m), pos, _mr, i);
                        if (length > 1 && length > max_length) {
                            seq_id = m + 1;  
                            max_pos = pos;  
                            max_length = length;
                        }
                    }
                }
            }
            if (max_length!=0) {
                delta_seq_id = seq_id - pre_seq_id;
                delta_length = max_length - 2;
                delta_pos = max_pos - pre_pos;
                pre_seq_id = seq_id;
                pre_pos = max_pos + max_length;
                secondMatchTotalLength += max_length;

                if (!misMatchEntry.isEmpty()) {
                    for (MatchEntry k:misMatchEntry) {
                        saveMatchEntry(list,k);
                    }
                    misMatchEntry.clear();
                }
                sbt.append(delta_seq_id).append(' ').append(delta_pos).append(' ').append(delta_length);
                list.add(sbt.toString());

                sbt.delete(0,sbt.length());
                i += max_length - 1;


            }
            else {
                misMatchEntry.add(_mr.get(i));
            }
        }
        if (i == _mr.size()-1)  misMatchEntry.add(_mr.get(i));
        if (!misMatchEntry.isEmpty()) {
            for (MatchEntry matchEntry : misMatchEntry) saveMatchEntry(list, matchEntry);
            misMatchEntry.clear();
        }
    }
}
