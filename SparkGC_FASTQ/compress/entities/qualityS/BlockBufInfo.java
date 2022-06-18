package compress.entities.qualityS;

public class BlockBufInfo {
    public int start;
    public int end;
    public int eline;
    public byte bucket;
    public MyByteBuffer in;       // uncompressed input
    public MyByteBuffer out;      // compressed output
    public String method;         // compression level or "" to mark end of data
    public BlockBufInfo() {}
}
