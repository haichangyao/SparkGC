package util;

public class reference_type implements java.io.Serializable{
    private  final int MAX_CHA_NUM = 1 << 28;//maximum length of a chromosome
    private  final int VEC_SIZE = 1 <<20; //length for other character arrays
    private static int kMerLen = 13; //the length of k-mer
    private static int kmer_bit_num = 2 * kMerLen; //bit numbers of k-mer
    private static int hashTableLen = 1 << kmer_bit_num; // length of hash table

    public int[] getRefLoc() {
        return refLoc;
    }

    public int getRefLoc_len() {
        return refLoc_len;
    }
    public void add_refLoc(int loc){refLoc[refLoc_len++] = loc;}

    public void setrefLoc_byturn(int loc,int len){refLoc[len] = loc;}
    public int getrefLoc_byturn(int len){return refLoc[len];}

    private  int refLoc[]= new int[MAX_CHA_NUM]; //reference hash location
    private int refLoc_len ;

    public int getRefBucket_Byturn(int len) {
        return refBucket[len];
    }

    public void setrefBucket_byturn(int loc,int len){refBucket[len] = loc;}

    private  int refBucket[]= new int[hashTableLen]; //reference hash bucket



    public void set_Ref_code_len(int ref_code_len) {
        this.ref_code_len = ref_code_len;
    }

    public static int getHashTableLen() {
        return hashTableLen;
    }

    public int getRef_code_len() {
        return ref_code_len;
    }

    public int getRef_low_len() {
        return ref_low_len;
    }

    private int ref_code_len;

    public void set_Ref_low_len(int ref_low_len) {
        this.ref_low_len = ref_low_len;
    }

    private int ref_low_len;

    public char get_Ref_code_Byturn(int len) {
        return ref_code[len];
    }

    public void set_Ref_code(char[] ref_code) {
        this.ref_code = ref_code;
    }

    public void set_Ref_code_byturn(char code,int len){ref_code[len] = code;}

    private char ref_code[]=new char[MAX_CHA_NUM];

    public int get_Ref_low_begin_byturn(int len) {
        return ref_low_begin[len];
    }

    public void set_Ref_low_begin(int[] ref_low_begin) {
        this.ref_low_begin = ref_low_begin;
    }

    public void set_Ref_low_begin_byturn(int begin,int len){ref_low_begin[len] = begin;}

    public void setRefLoc(int[] refLoc) {
        this.refLoc = refLoc;
    }

    public void setRefBucket(int[] refBucket) {
        this.refBucket = refBucket;
    }

    private int[] ref_low_begin = new int[VEC_SIZE];

    public int get_Ref_low_length_byturn(int len) {
        return ref_low_length[len];
    }

    public void set_Ref_low_length(int[] ref_low_length) {
        this.ref_low_length = ref_low_length;
    }

    public void set_Ref_low_length_byturn(int length,int len){ref_low_length[len] = length;}

    private int[] ref_low_length = new int[VEC_SIZE];


}
