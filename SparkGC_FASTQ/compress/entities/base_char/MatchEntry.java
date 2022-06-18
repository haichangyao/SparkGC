package compress.entities.base_char;

public class MatchEntry implements java.io.Serializable{

    private int pos;
    private int length;
    private String misStr;


    public int getPos() {
        return pos;
    }

    public int getLength() {
        return length;
    }

    public String getMisStr() {
        return misStr;
    }



    public void setLength(int length) {
        this.length = length;
    }

    public void setMisStr(String misStr) {
        this.misStr = misStr;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }
}
