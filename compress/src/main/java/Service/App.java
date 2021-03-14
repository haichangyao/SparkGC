package Service;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import pojo.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.util.*;

import static java.lang.Math.*;
import static org.apache.spark.api.java.StorageLevels.*;

public class App {

    private static int k = 28;//
    private static final int VEC_SIZE = 1 << 20;
    private static final int kMerLen = 13; //the length of k-mer
    private static final int kmer_bit_num = 2 * kMerLen; //bit numbers of k-mer
    private static final int hashTableLen = 1 << kmer_bit_num; // length of hash table
    private static final int seqBucketLen = getNextPrime(1<<20);
    private static reference_type createRefBroadcast(String filename) throws IOException{
        String str;
        char []cha;
        int _seq_code_len = 0, _ref_low_len = 1, letters_len = 0;//record lowercase from 1, diff_lowercase_loc[i]=0 means mismatching
        boolean flag = true;
        char temp_cha;
        File file = new File(filename);

        BufferedReader br = new BufferedReader(new FileReader(file));
        str = br.readLine();
        if(str.contains("chr14")||str.contains("chr15")||str.contains("chr16")||str.contains("chr17")
                ||str.contains("chr18")||str.contains("chr19")||str.contains("chr20")||str.contains("chr21")
                ||str.contains("chr22")||str.contains("chrY")) k = 27;
        else k = 28;
        reference_type ref = new reference_type(k);
        while((str=br.readLine())!=null){
            cha = str.toCharArray();
            for (char a: cha) {
                temp_cha=a;
                if(Character.isLowerCase(temp_cha)){
                    if (flag) //previous is uppercase
                    {
                        flag = false; //change status of flag
                        ref.set_Ref_low_begin_byturn(letters_len,_ref_low_len);
                        letters_len = 0;
                    }
                    temp_cha = Character.toUpperCase(temp_cha);
                }
                else {
                    if (!flag)  //previous is lowercase
                    {
                        flag = true;
                        ref.set_Ref_low_length_byturn(letters_len,_ref_low_len);
                        _ref_low_len++;
                        letters_len = 0;
                    }
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
        System.out.println("Compression begins.");
        kMerHashingConstruct(ref);
        return ref;
    }

    private static compression_type createSeqRDD(Iterator<Tuple2<Text,Text>> s,Broadcast<Integer> _k){
        int letters_len = 0, n_letters_len = 0;
        boolean flag = true, n_flag = false;
        char temp_cha;

        String str;
        char[] cha;      //the content of one line
        compression_type c = new compression_type(_k.value());
        c.identifier = s.next()._1.toString();

        while (s.hasNext()){
            str = s.next()._1.toString();
            if(c.line==0)
                c.line = str.length();
            cha=str.toCharArray();
            for (int i = 0; i < cha.length; i++){
                temp_cha = cha[i];
                if (Character.isLowerCase(temp_cha)) {
                    if (flag) {   //previous is uppercase
                        flag = false;
                        c.addSeq_low_begin(letters_len);
                        letters_len = 0;
                    }
                    temp_cha = Character.toUpperCase(temp_cha);
                }
                else {
                        if (!flag) {   //previous is lowercase
                            flag = true;
                            c.addSeq_low_length(letters_len);
                            letters_len = 0;
                        }
                }
                letters_len++;

                if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T')
                    c.addSeq_code(temp_cha);
                else if (temp_cha != 'N') {
                    c.addSpe_cha_pos(c.getSeq_len());
                    c.addSpe_cha_ch(temp_cha - 'A');
                }
                if (!n_flag) {
                    if (temp_cha == 'N') {
                        c.addnCha_begin(n_letters_len);
                        n_letters_len = 0;
                        n_flag = true;
                    }
                }
                else {
                    if (temp_cha != 'N') {
                        c.addnCha_length(n_letters_len);
                        n_letters_len = 0;
                        n_flag = false;
                    }
                }
                n_letters_len++;
            }
        }
        if (!flag)
            c.addSeq_low_length(letters_len);

        if (n_flag)
            c.addnCha_length(n_letters_len);

        for (int i = c.getSpe_cha_len() - 1; i > 0; i--)
            c.setSpe_cha_pos_Byturn(i,c.getSpe_cha_pos_Byturn(i)-c.getSpe_cha_pos_Byturn(i-1));
        return c;
    }

    private static void kMerHashingConstruct(reference_type ref){
        //initialize the point array
        for (int i = 0; i < hashTableLen; i++)
            ref.setrefBucket_byturn(-1,i);
        int value = 0;
        int step_len = ref.getRef_code_len() - kMerLen + 1;

        //calculate the value of the first k-mer
        for (int k = kMerLen - 1; k >= 0; k--) {
            value <<= 2;
            value += integerCoding(ref.get_Ref_code_Byturn(k));
        }
        ref.setrefLoc_byturn(ref.getRefBucket_Byturn(value),0);
        ref.setrefBucket_byturn(0,value);

        int shift_bit_num = (kMerLen * 2 - 2);
        int one_sub_str = kMerLen - 1;

        //calculate the value of the following k-mer using the last k-mer
        for (int i = 1; i < step_len; i++) {
            value >>= 2;
            value += (integerCoding(ref.get_Ref_code_Byturn(i + one_sub_str))) << shift_bit_num;
            ref.setrefLoc_byturn(ref.getRefBucket_Byturn(value),i);    //refLoc[i] record the list of same values
            ref.setrefBucket_byturn(i,value);
        }
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

    private static int integerCoding(char ch) { //encoding ACGT
        switch (ch) {
            case 'A': return 0;
            case 'C': return 1;
            case 'G': return 2;
            case 'T': return 3;
            default : return -1;
        }
    }

    private static void seqLowercaseMatching(compression_type tar,reference_type ref) {
        int start_position = 1;
        int _diff_low_len = 0;

        //initialize the diff_low_loc, diff_low_loc record the location of the same lowercase element
        int []diff_low_begin = new int[VEC_SIZE];
        int []diff_low_length = new int[VEC_SIZE];

        for (int i = 0; i < tar.getSeq_low_len(); i++) {
            //search from the start_position to the end
            for (int j = start_position; j < ref.getRef_low_len(); j++) {
                if ((tar.getSeq_low_begin_Byturn(i)== ref.get_Ref_low_begin_byturn(j))
                        && (tar.getSeq_low_length_Byturn(i) == ref.get_Ref_low_length_byturn(j))) {
                    tar.setLow_loc_Byturn(j,i);//low_loc[i] = j;
                    start_position = j + 1;
                    break;
                }
            }

            //search from the start_position to the begin
            if (tar.getLow_loc_Byturn(i) == 0) {
                for (int j = start_position - 1; j > 0; j--) {
                    if ((tar.getSeq_low_begin_Byturn(i) == ref.get_Ref_low_begin_byturn(j))
                            && (tar.getSeq_low_length_Byturn(i) == ref.get_Ref_low_length_byturn(j))) {
                        tar.setLow_loc_Byturn(j,i);
                        start_position = j + 1;
                        break;
                    }
                }
            }

            //record the mismatched information
            if (tar.getLow_loc_Byturn(i) == 0) {
                diff_low_begin[_diff_low_len] = tar.getSeq_low_begin_Byturn(i);
                diff_low_length[_diff_low_len++] = tar.getSeq_low_length_Byturn(i);
            }
        }
        tar.lowerInicial();
        tar.setDiff_low_len(_diff_low_len);
        tar.setDiff_low_begin(diff_low_begin);
        tar.setDiff_low_length(diff_low_length);
    }

    private static List<Integer> runLengthCoding(int []vec ,int length, int tolerance) {
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

    private static void saveOtherData(compression_type tar,reference_type ref,List<String> other) {
        //lowercase information
        int flag = 0;

        if (tar.getSeq_low_len() > 0 && ref.getRef_low_len() > 0) {
            seqLowercaseMatching(tar,ref);
            if ((2 * tar.getDiff_low_len()) < tar.getSeq_low_len()) {
                flag = 1;
                other.add(String.valueOf(flag));
                List<Integer> loc = runLengthCoding(tar.getLow_loc(),tar.getSeq_low_len(),1);
                other.add(String.valueOf(loc.size()));
                for (Integer a: loc) {
                    other.add(String.valueOf(a));
                }
                other.add(String.valueOf(tar.getDiff_low_len()));
                for (int i = 0;i<=tar.getDiff_low_len();i++){
                    other.add(String.valueOf(tar.getDiff_low_begin_byturn(i)));
                    other.add(String.valueOf(tar.getDiff_low_length_byturn(i)));
                }
           }
        }
        if (flag == 0) {
            other.add(String.valueOf(flag));
            other.add(String.valueOf(tar.getSeq_low_len()));
            for (int i = 0;i<tar.getSeq_low_len();i++){
                other.add(String.valueOf(tar.getSeq_low_begin_Byturn(i)));
                other.add(String.valueOf(tar.getSeq_low_length_Byturn(i)));
            }
        }
        tar.setDiff_low_len(0);
        tar.setDiff_low_begin(null);
        tar.setDiff_low_length(null);
        tar.setSeq_low_begin(null);
        tar.setSeq_low_length(null);
        tar.setLow_loc(null);

        //N character
        other.add(String.valueOf(tar.getnCha_len()));
        for(int i = 0;i<tar.getnCha_len();i++){
            other.add(String.valueOf(tar.getnCha_begin_Byturn(i)));
            other.add(String.valueOf(tar.getnCha_length_Byturn(i)));
        }
        tar.setnCha_begin(null);
        tar.setnCha_length(null);

        //special character
        other.add(String.valueOf(tar.getSpe_cha_len()));
        if(tar.getSpe_cha_len()>0){
            for(int i = 0;i<tar.getSpe_cha_len();i++){
                other.add(String.valueOf(tar.getSpe_cha_pos_Byturn(i)));
                other.add(String.valueOf(tar.getSpe_cha_ch_Byturn(i)));
            }
        }
        tar.setSpe_cha_pos(null);
        tar.setSpe_cha_ch(null);
    }

    private static List<MatchEntry> codeFirstMatch(compression_type tar, reference_type ref) {
        int pre_pos = 0;
        int min_rep_len = 15;
        int step_len = tar.getSeq_len() - kMerLen + 1;
        int max_length, max_k;
        int i, id, k, ref_idx, tar_idx, length, cur_pos, tar_value;
        StringBuilder mismatched_str = new StringBuilder();
        List<MatchEntry> mr = new ArrayList<>();
        MatchEntry me ;
        for (i = 0; i < step_len; i++) {
            tar_value = 0;
            //calculate the hash value of the first k-mer
            for (k = kMerLen - 1; k >= 0; k--) {
                tar_value <<= 2;
                tar_value += integerCoding(tar.getSeq_code_byturn(i+k));
            }

            id = ref.getRefBucket_Byturn(tar_value);
            if (id > -1) {                      //there is a same k-mer in ref_seq_code
                max_length = -1;
                max_k = -1;
                //search the longest match in the linked list
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
                //exist a k-mer, its length is larger then min_rep_len
                if (max_length > -1) {
                    me= new MatchEntry();
                    //then save matched information
                    cur_pos = max_k - pre_pos;      //delta-coding for cur_pos
                    me.setPos(cur_pos);
                    me.setLength(max_length - min_rep_len);
                    me.setMisStr(mismatched_str.toString());
                    mr.add(me);
                    i += max_length;
                    pre_pos = max_k + max_length;
                    mismatched_str.delete(0,mismatched_str.length());
                    if (i < tar.getSeq_len())
                        mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;
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
            me.setLength(-min_rep_len);                 //no match information, not 0 ,is -min_rep_len;
            me.setMisStr(mismatched_str.toString());
            mr.add(me);
        }
        tar.setSeq_code(null);
        return mr;
    }

    //calculate the hash value of MatchEntry
    private static int getHashValue(MatchEntry me) {
        int result = 0;
        for (int i = 0; i < me.getMisStr().length(); i++) {
            result += me.getMisStr().charAt(i) * 92083;
        }
        result += me.getPos() * 69061 + me.getLength() * 51787;
        result %= getNextPrime(1<<20);
        return result;
    }

    //hash indexing building for matchResult
    private static void matchResultHashConstruct(List<MatchEntry> matchResult,Map<Integer,int[]> seqBucketVec,
                                                 Map<Integer,List<Integer>> seqLocVec,int Num) {
        int hashValue1, hashValue2, hashValue;
        List<Integer> seqLoc = new ArrayList<>(VEC_SIZE);
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

    private static int getMatchLength(List <MatchEntry> ref_me, int ref_idx, List <MatchEntry> tar_me, int tar_idx) {
        int length = 0;
        while (ref_idx < ref_me.size() && tar_idx < tar_me.size() && compareMatchEntry(ref_me.get(ref_idx++), tar_me.get(tar_idx++)))
            length++;
        return length;
    }

    private static Boolean compareMatchEntry(MatchEntry ref, MatchEntry tar) {
        return  ref.getPos() == tar.getPos() && ref.getLength() == tar.getLength() && ref.getMisStr().equals(tar.getMisStr());
    }

    private static void saveMatchEntry(List<String> list, MatchEntry matchEntry) {
        StringBuilder sbf = new StringBuilder();
        if(!matchEntry.getMisStr().isEmpty()){
            list.add(matchEntry.getMisStr());
        }
        sbf.append(matchEntry.getPos()).append(' ').append(matchEntry.getLength());
        list.add(sbf.toString());

    }

    private static void codeSecondMatch( List<MatchEntry> _mr, int seqNum, Map<Integer,int[]> seqBucket_vec,
                                         Map<Integer,List<Integer>> seqLoc_vec , Map<Integer,List<MatchEntry>> matchResult_vec,
                                         List<String> list,int secNum) {
        int hashValue;
        int pre_seq_id=1;
        int max_pos=0, pre_pos=0, delta_pos, length, max_length, delta_length, seq_id=0, delta_seq_id;
        int id, pos;
        int i;
        StringBuilder sbt = new StringBuilder();
        ArrayList<MatchEntry> misMatchEntry = new ArrayList<>();
        for (i = 0; i < _mr.size()-1; i++) {

            //calculate the hash value of the to-be-compressed matchentry
            if(_mr.size()<2) hashValue = abs(getHashValue(_mr.get(i))) % seqBucketLen;
            else hashValue = abs(getHashValue(_mr.get(i)) + getHashValue(_mr.get(i+1))) % seqBucketLen;
            max_length = 0;
            //search for the identical matchentry
            for (int m = 0; m < min( seqNum-1, secNum); m++) {
                id = seqBucket_vec.get(m)[hashValue];
                if (id!=-1) {
                    for (pos = id; pos!=-1; pos = seqLoc_vec.get(m).get(pos)) {
                        length = getMatchLength(matchResult_vec.get(m), pos, _mr, i);
                        if (length > 1 && length > max_length) {
                            seq_id = m + 1;  //the m-th sequence in seqBucket, but actually m+1 in seqName
                            max_pos = pos;
                            max_length = length;
                        }
                    }
                }
            }

            if (max_length!=0) {
                delta_seq_id = seq_id - pre_seq_id;//delta encoding
                delta_length = max_length - 2;//delta encoding
                delta_pos = max_pos - pre_pos;//delta encoding
                pre_seq_id = seq_id;
                pre_pos = max_pos + max_length;

                //firstly save mismatched matchentry！
                if (!misMatchEntry.isEmpty()) {
                    for (MatchEntry k:misMatchEntry) {
                        saveMatchEntry(list,k);
                    }
                    misMatchEntry.clear();
                }
                //secondly save matched matchentry！
                sbt.append(delta_seq_id).append(' ').append(delta_pos).append(' ').append(delta_length);
                list.add(sbt.toString());
                sbt.delete(0,sbt.length());
                i += max_length - 1;
            }
            else {
                misMatchEntry.add(_mr.get(i));
            }
        }
        //save the rest of matchentry
        if (i == _mr.size()-1)  misMatchEntry.add(_mr.get(i));
        if (!misMatchEntry.isEmpty()) {
            for (MatchEntry matchEntry : misMatchEntry) saveMatchEntry(list, matchEntry);
            misMatchEntry.clear();
        }
    }




    public static void compress(String ref_file,String tar_file,String out_path) throws IOException{
        long startTime = System.currentTimeMillis();
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", mykryo.class.getName()).setAppName("geneCompress").setMaster("yarn");
        sparkConf.set("spark.kryoserializer.buffer.max","2000").set("spark.driver.maxResultSize", "6g").set("spark.shuffle.sort.bypassMergeThreshold","20");
        sparkConf.set("spark.default.parallelism", "40").set("spark.shuffle.file.buffer","3000").set("spark.reducer.maxSizeInFlight", "1000");
        sparkConf.set("spark.broadcast.blockSize", "256m");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.split.minsize","268435456");

        Path path2 = new Path(out_path);

        int secNum = 45;
        FileSystem fs =FileSystem.get(jsc.hadoopConfiguration());
        if(fs.exists(path2)){
            fs.delete(path2,true);
        }
        reference_type ref = createRefBroadcast(ref_file);
        //create broadcast valuables
        final Broadcast<reference_type> referenceTypeBroadcast = jsc.broadcast(ref);
        final Broadcast<Integer> kk = jsc.broadcast(k);
        final Broadcast<Integer> secNumber = jsc.broadcast(secNum);

        long stage1EndTime = System.currentTimeMillis();
        System.out.println("----------------The initial time is ----------------------------" + (stage1EndTime-startTime)/1000 + " s.");

        JavaPairRDD<Text,Text> ta_rdd = jsc.newAPIHadoopFile(tar_file, KeyValueTextInputFormat.class, Text.class ,Text.class ,jsc.hadoopConfiguration());
        JavaRDD<Tuple3<Integer,List<MatchEntry>,List<String>>> first_match = ta_rdd.mapPartitions(s1->{
            List<compression_type> list = new ArrayList<>();
            list.add(createSeqRDD(s1,kk));
            return list.iterator();
        }).mapPartitionsWithIndex((v1,v2)->{
            List<Tuple3<Integer,List<MatchEntry>,List<String>>> li = new ArrayList<>();
            List<String> list = new ArrayList<>();
            Tuple3<Integer,List<MatchEntry>,List<String>> tu2 =null;
            while(v2.hasNext()&&referenceTypeBroadcast.value()!=null) {
                compression_type tar = v2.next();
                list.add(tar.identifier);
                list.add(String.valueOf(tar.line));
                saveOtherData(tar,referenceTypeBroadcast.value(),list);
                tu2 = new Tuple3<>(v1, codeFirstMatch(tar, referenceTypeBroadcast.value()),list);
            }
            li.add(tu2);
            return li.iterator();
        },true).persist(MEMORY_ONLY_SER);

        JavaRDD<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> sec = first_match.filter(s->s._1()<=secNumber.getValue())
                .coalesce(1,true).mapPartitions(s->{
                    int sn = secNumber.getValue();
                    List<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> list = new ArrayList<>();
                    Map<Integer,List<MatchEntry>> MatchList = new HashMap<>();
                    Map<Integer,List<Integer>> seqLoc = new HashMap<>(sn+2);
                    Map<Integer,int[]> seqBucket = new HashMap<>(sn+2);
                while (s.hasNext()){
                    Tuple3<Integer,List<MatchEntry>,List<String>> l = s.next();
                    matchResultHashConstruct(l._2(),seqBucket,seqLoc,l._1());
                    MatchList.put(l._1(),l._2());
                }
                list.add(new Tuple3<>(MatchList,seqBucket,seqLoc));
                return list.iterator();
        });

        Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>> ref2 = sec.first();

        long stage2EndTime = System.currentTimeMillis();
        System.out.println("The first-order compression time is " + (stage2EndTime-stage1EndTime)/1000 + " s.");

        ref.set_Ref_code(null);
        ref.setRefLoc(null);
        ref.setRefBucket(null);
        final Broadcast<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> sec_ref = jsc.broadcast(ref2);

        first_match.mapPartitions(s->{
            int sn = secNumber.getValue();
            List<String> list = new ArrayList<>();
            Tuple3<Integer,List<MatchEntry>,List<String>> tar;
            while (s.hasNext()&&sec_ref.value()!=null) {
                tar = s.next();
                list = tar._3();
                if(tar._1() == 0){
                    for (int i = 0; i < tar._2().size(); i++) {
                        saveMatchEntry(list, tar._2().get(i));
                    }
                }else
                    codeSecondMatch(tar._2(),tar._1()+1,
                            sec_ref.value()._2(),sec_ref.value()._3(),sec_ref.value()._1(), list, sn);
            }
            return list.iterator();
        }).saveAsTextFile(path2.toString());
        sec_ref.unpersist();
        first_match.unpersist();

        long stage3EndTime = System.currentTimeMillis();
        System.out.println("The second-order compression time is " + (stage3EndTime-stage2EndTime)/1000 + " s.");
    }
}

