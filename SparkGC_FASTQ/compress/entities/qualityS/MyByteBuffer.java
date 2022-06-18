package compress.entities.qualityS;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class MyByteBuffer {
    private ByteBuf bb;
    private int rpos;       // index of next byte to read, rpos < wpos or return EOF.

    public MyByteBuffer(){
        bb = Unpooled.buffer(34*1024*1024);
        rpos = 0;
    }

    public MyByteBuffer(int initial){
        bb = Unpooled.buffer(initial);
        rpos = 0;
    }

    public void put(char c) throws Exception {  // write 1 byte
        if(bb.writableBytes() >= 1) {
            bb.writeByte(c);
        } else {
            System.out.println("bb.writableBytes()="+bb.writableBytes());
            throw new Exception("no bytes to be written");
        }

    }

    public void put(int c) throws Exception {  // write 1 byte
        if(bb.writableBytes() >= 1) {
            bb.writeByte(c);
        } else {
            System.out.println("bb.writableBytes()="+bb.writableBytes());
            throw new Exception("no bytes to be written");
        }

    }

    public void write(char[] buf) throws Exception {
        if (buf.length < 1) return;
        for(int i = 0; i < buf.length; i++) {
            put(buf[i]);
        }
    }
	
    public int get() {
        if(bb.readableBytes() > 0) {
            return  bb.readByte()&0xff ;
        } else {
            return -1;
        }
    }

    public byte[] getAll() {
        int len = size();
        byte[] buff = new byte[len];
        int size = 0;

        int out = 0;
        while ((out = get()) != -1) {
            buff[size++] = (byte)out;
        }

        assert len == buff.length;
        assert len == size;

        return buff;
    }

    public byte[] readAll() throws Exception {
        if(!bb.nioBuffer().hasArray()) {
            throw new Exception("buffer have no hasArray");
        }
        return bb.nioBuffer().array();


    }


    public int size() {
        return bb.readableBytes();
    }

    public int writableBytes() {
        return bb.writableBytes();
    }

    public void resize(int size) {
        bb.capacity(size);
    }

    public void swap(MyByteBuffer b2){
        MyByteBuffer temp = new MyByteBuffer();
        temp.bb = this.bb;
        temp.rpos = this.rpos;
        this.bb = b2.bb;
        this.rpos = b2.rpos;
        b2.bb = temp.bb;
        b2.rpos = temp.rpos;
    }



}
