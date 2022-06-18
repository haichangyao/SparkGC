package compress.entities.base_char;

public class Bases_Seq implements java.io.Serializable{


    private  char[] seq_code;
    private int seq_len;

    public Bases_Seq(){
        SeqInitial();
    }
    private void SeqInitial(){
        int VEC_SIZE = 1 <<17;//length for other character arrays//131072

        seq_code= new char[10100000];


    public int getSeq_len() {
        return seq_len;
    }
    public char getSeq_code_byturn(int len) {
        return seq_code[len];
    }
    public void addSeq_code(char seq_code) {
        this.seq_code[seq_len++] = seq_code;
    }
    public void setSeq_code(char[] seq_code) {
        this.seq_code = seq_code;
    }
}
