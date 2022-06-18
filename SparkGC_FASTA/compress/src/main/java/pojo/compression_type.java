package pojo;

public class compression_type implements java.io.Serializable{

    private  int MAX_CHA_NUM ;//maximum length of a chromosome
    private  final int VEC_SIZE = 1 <<20; //length for other character arrays

    public int line;
    private int seq_len;

    public compression_type(int k) {
        MAX_CHA_NUM = 1<<k;
        SeqInitial();
    }

    private  char []seq_code;//mismatched subsequence

    public int getSeq_len() {
        return seq_len;
    }

    public char getSeq_code_byturn(int len) {
       return seq_code[len];
    }

    public void addSeq_code(char seq_code) {
        this.seq_code[seq_len++] = seq_code;
    }

    public int[] getLow_loc() {
        return low_loc;
    }

    public void addLow_loc(int low_loc) {
        this.low_loc[low_loc_len++] = low_loc;
    }

    public int getSeq_low_begin_Byturn(int len) {
        return seq_low_begin[len];
    }

    public void addSeq_low_begin(int begin) {
        this.seq_low_begin[seq_low_len] = begin;
    }

    public void setSeq_low_begin_Byturn(int begin,int len) {seq_low_begin[len] = begin;}

    public int getSeq_low_length_Byturn(int len) {
        return seq_low_length[len];
    }

    public void addSeq_low_length(int length) {
        this.seq_low_length[seq_low_len++] = length;
    }

    public int getnCha_begin_Byturn(int len) {
        return nCha_begin[len];
    }

    public void addnCha_begin(int begin) {
        this.nCha_begin[nCha_len] = begin;
    }

    public int getnCha_length_Byturn(int len) {
        return nCha_length[len];
    }

    public void addnCha_length(int length) {
        this.nCha_length[nCha_len++] = length;
    }

    public void addSpe_cha_pos(int spe_cha_pos) {
        this.spe_cha_pos[spe_cha_len] = spe_cha_pos;
    }

    public void setSpe_cha_pos(int[] spe_cha_pos) {
        this.spe_cha_pos = spe_cha_pos;
    }

    public int getSpe_cha_pos_Byturn(int len) {
        return  spe_cha_pos[len];
    }

    public void setSpe_cha_pos_Byturn(int len,int pos) {
        this.spe_cha_pos[len] = pos;
    }

    public int getSpe_cha_ch_Byturn(int len) {
        return spe_cha_ch[len];
    }

    public void addSpe_cha_ch(int spe_cha_ch) {
        this.spe_cha_ch[spe_cha_len++] = spe_cha_ch;
    }

    private void SeqInitial(){
        nCha_begin= new int[VEC_SIZE/2];
        nCha_length= new int[VEC_SIZE/2];
        spe_cha_pos= new int[VEC_SIZE/2];
        seq_code= new char[MAX_CHA_NUM];
        seq_low_begin= new int[VEC_SIZE];
        seq_low_length= new int[VEC_SIZE];
        spe_cha_ch= new int[VEC_SIZE/2];
        low_loc = new int[VEC_SIZE];
    }

    public void setLow_loc_Byturn(int low_loc,int len) {
        this.low_loc[len] = low_loc;
    }

    public int getLow_loc_Byturn(int len) {
        return this.low_loc[len];
    }

    private int []low_loc; //lowercase tuple location
    private int low_loc_len;

    private int []seq_low_begin;

    public int getSeq_low_len() {
        return seq_low_len;
    }

    private int seq_low_len;

    private int []seq_low_length;

    public int getnCha_len() {
        return nCha_len;
    }

    private int nCha_len;

    public void setSeq_code(char[] seq_code) {
        this.seq_code = seq_code;
    }

    public void setLow_loc(int[] low_loc) {
        this.low_loc = low_loc;
    }

    public void setSeq_low_begin(int[] seq_low_begin) {
        this.seq_low_begin = seq_low_begin;
    }

    public void setSeq_low_length(int[] seq_low_length) {
        this.seq_low_length = seq_low_length;
    }

    public void setnCha_begin(int[] nCha_begin) {
        this.nCha_begin = nCha_begin;
    }

    public void setnCha_length(int[] nCha_length) {
        this.nCha_length = nCha_length;
    }

    private int []nCha_begin;
    private int []nCha_length;
    private int []spe_cha_pos;

    public int getSpe_cha_len() {
        return spe_cha_len;
    }

    private int spe_cha_len;

    public void setSpe_cha_ch(int[] spe_cha_ch) {
        this.spe_cha_ch = spe_cha_ch;
    }

    private int []spe_cha_ch;

    public void setDiff_low_begin(int[] diff_low_begin) {
        this.diff_low_begin = diff_low_begin;
    }

    public void setDiff_low_length(int[] diff_low_length) {
        this.diff_low_length = diff_low_length;
    }

    public int getDiff_low_begin_byturn(int len) {
        return diff_low_begin[len];
    }

    public int getDiff_low_length_byturn(int len) {
        return diff_low_length[len];
    }

    private int[] diff_low_begin ;
    private int[] diff_low_length;

    public int getDiff_low_len() {
        return diff_low_len;
    }

    //    private int diff_low_length[];
    private int diff_low_len;

    public void lowerInicial(){
        diff_low_begin = new int[VEC_SIZE/2];
        diff_low_length = new int[VEC_SIZE/2];
    }

    public void setDiff_low_len(int diff_low_len) {
        this.diff_low_len = diff_low_len;
    }
    public   String identifier;
}
