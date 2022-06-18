package compress.util.quality_func;

import compress.entities.qualityS.BlockBufInfo;
import compress.entities.qualityS.MyByteBuffer;
import compress.entities.qualityS.QualityScores;
import compress.util.DealReads4;
import scala.Tuple2;

import java.io.*;
import java.util.*;

import static compress.entities.qualityS.QualityScores.SAMPLE_NUM;
import static java.lang.StrictMath.max;

public class QualityCompressor {

    public static int[] ByteCount = new int[2];

    public static final int BUFFER_SIZE = 1 << 25;//32MB
    private static final int K = 2;
    private long base = 1;

    private BufferedWriter bw ;
    private final float alpha = (float) 0.1;
    private char score = 0;


    List<Tuple2<String, byte[]>> outList = null;
    public List<Tuple2<String, byte[]>> getOutList() {
        return outList;
    }

    public MyByteBuffer[] sb;
    public int[] cur = new int[3];
    public int[] pre = new int[2];
    public int[] eline = new int[2];
    private int flag = 0;

    public QualityCompressor(BufferedWriter output) throws IOException {
        bw = output;
        sb = new MyByteBuffer[2];
        sb[0] = new MyByteBuffer();
        sb[1] = new MyByteBuffer();
        for(int j = 1; j < K; ++j) base = base << 7;//base=1000 0000
        base -= 1;//0111 1111

    public QualityCompressor(int listLen) {
        sb = new MyByteBuffer[2];
        sb[0] = new MyByteBuffer();
        sb[1] = new MyByteBuffer();
        for(int j = 1; j < K; ++j) base = base << 7;
        base -= 1;

        outList = new ArrayList<>(listLen);
    }


    public void get_score(QualityScores qs){
        int[] score_cnt = new int[128];
        for(String s : qs.getSample()) {
            for(char c : s.toCharArray()) ++score_cnt[c];
        }

        int max = -1;
        int maxChar = 0;
        for(int i = 0; i < score_cnt.length; i++) {
            if(score_cnt[i] > max) {
                max = score_cnt[i];
                maxChar = i;
            }
        }
        score = (char) maxChar;

    }

    public double get_table(QualityScores qs){

        Map<Long, Integer> mp = new HashMap<>();
        int tot = 0;
        for(String s : qs.getSample()) {
            for(int i = K -1; i < s.length(); ++i) {
                long val = 0;
                for(int l = i+1- K; l <= i; ++l) val = val << 7 | s.charAt(l);
                if(mp.containsKey(val)){
                    mp.put(val, mp.get(val)+1);
                    continue;
                }
                mp.put(val, 1);
            }
            tot += s.length()+1- K;
        }
        List<Tuple2<Integer, Long>> vec = new ArrayList<>();
        for(Map.Entry<Long,Integer> m : mp.entrySet()) {
            vec.add(new Tuple2<>(m.getValue(), m.getKey()));
        }

        vec.sort((o1,o2)-> {
            if(o1._1.equals(o2._1)) return o1._2<o2._2?-1:((o1._2.equals(o2._2))?0:1);
            return o1._1.compareTo(o2._1);
        });

        int cnt = (int)(tot * 0.7);
        for (int i = 0; cnt > 0; ++i) {
            Tuple2<Integer,Long> it = vec.get(i);
            qs.addTableByturn(it._2, it._1/(double)tot);
            cnt -= it._1;
        }

        double mx = 0;
        for(String s : qs.getSample()) {
            double score = 0;
            for(int i = K -1; i < s.length(); ++i) {
                long val = 0;
                for(int l = i+1- K; l <= i; ++l) val = val << 7 | s.charAt(l);
                if(qs.getTable().containsKey(val)) score += qs.getTable().get(val);
            }
            mx = max(mx, score/(s.length()+1- K));
        }
        return mx;
    }

    private void pack(MyByteBuffer in, int bucket) throws Exception {
        if(in.size()==0) return;
        MyByteBuffer out = new MyByteBuffer();
        int len = 0; // pending output bytes
        int count = 0;//for debug
        int countOutput = 0;//for debug

        int j = 0, k = 0, l2 = max(33, score-7), l3 = l2 + 4, r = l2 + 7; 
        for (int c = 0; (c = in.get()) != -1; k = j, j = c) {
            count++;

            if (len == 0 && (c == score || c >= l2 && c <= r)) ++len;
            else if (len == 1 && (c == score && j == score || c >= l3 && c <= r && j >= l3 && j <= r)) ++len;
            else if (len >= 2 && len<55 && k == score && j == score && c == score) ++len;
            else {  // must write pending output
                ++len;  // c is pending
                if (len>2 && j == score && k == score || len==2 && j == score){
                    out.put(199 + len);len = 1;
                    countOutput++;
                }

                if (len == 3) {
                    if (c >= l3 && c <= r){
                        out.put(137 + (k - l3) + 4 * (j - l3) + 16 * (c - l3));
                        len = 0;
                        countOutput++;
                    }
                    else{
                        out.put(73 + (k - l2) + 8 * (j - l2));
                        len = 1;
                        countOutput++;
                    } 
                }
                if (len == 2) {
                    if (c >= l2 && c <= r) {
                        out.put(73 + (j - l2) + 8 * (c - l2));
                        len = 0;
                        countOutput++;
                    }
                    else {
                        out.put(j - 32);
                        len = 1;
                        countOutput++;
                    }
                }
                if (len == 1) {
                    if (c == 10) {
                        len = j = 0;
                        k = 0;
                        out.put(0);
                        countOutput++;
                        continue;
                    }
                    if (c<l2 || c>r) {
                        out.put(c - 32);
                        len = 0;
                        countOutput++;
                    }
                }
            }
        }
        assert countOutput == out.size();
        out.swap(in);
        assert countOutput == in.size();
    }

    public boolean qs_compress(String str, QualityScores qs, int k) throws Exception {
        qs.addSampleByturn(str);
        if(qs.getSample().size() == SAMPLE_NUM){
            get_score(qs);
            qs.setBoarder(get_table(qs) * alpha);

            bw.write(DealReads4.input[k].getAbsolutePath() + " " + k + " " + score);
            bw.newLine();
            bw.flush();

            compress_qs(qs);
        }
        return  (qs.getSample().size() == SAMPLE_NUM);
    }

    public boolean qs_compressForSpark(String str, QualityScores qs) throws Exception {
        qs.addSampleByturn(str);
        if(qs.getSample().size() == SAMPLE_NUM){
            get_score(qs);
            qs.setBoarder(get_table(qs) * alpha);

            System.out.println("boarder:" + qs.getBoarder());
            compress_qsForSpark(qs);
        }
        return  (qs.getSample().size() == SAMPLE_NUM);
    }

    private void compress_qs(QualityScores qs) throws Exception {
        for(int i = 0; i < qs.getSample().size(); i++) {
            dealing(qs.getSample().get(i), qs);
        }
    }

    private void compress_qsForSpark(QualityScores qs) throws Exception {

        for(int i = 0; i < qs.getSample().size(); i++) {
            dealingForSpark(qs.getSample().get(i), qs);
        }




    }

    public void dealing(String s, QualityScores qs) throws Exception {
        long val = 0;
        for(int j = 0; j < K -1; ++j) val = val << 7 | s.charAt(j);
        double score2 = 0;
        for(int j = K -1; j < s.length(); ++j) {
            val = (val & base) << 7 | s.charAt(j);
            if(qs.getTable().containsKey(val)){
                double it = qs.getTable().get(val);
                score2+=it;
            }
        }

        byte res = 0;
        if(score2 < qs.getBoarder()*(s.length()+1- K)) res = 1;

        sb[res].write(s.toCharArray());
        ByteCount[res] += s.length();
        sb[res].put('\n');
        ByteCount[res] += 1;
        if(res > 0) {
            sb[0].put('\n');
            ByteCount[0] += 1;
        }


        if(sb[res].size() > BUFFER_SIZE) {
            writeOneMyByteBufferToFile(sb[res],  res, pre[res], cur[res], eline[res]);
            sb[res] = new MyByteBuffer();
            if(res==1) ++flag;
            else flag = 0;
        }
        if(flag >= 3) {
            writeOneMyByteBufferToFile(sb[0], (byte) 0, pre[0], cur[0], eline[0]);
            sb[res] = new MyByteBuffer();
            flag = 0;
        }
    }


    public void dealingForSpark(String s, QualityScores qs) throws Exception {
        long val = 0;
        for(int j = 0; j < K -1; ++j) val = val << 7 | s.charAt(j);
        double score2 = 0;
        for(int j = K -1; j < s.length(); ++j) {
            val = (val & base) << 7 | s.charAt(j);
            if(qs.getTable().containsKey(val)){
                double it = qs.getTable().get(val);
                score2+=it;
            }
        }

        byte res = 0;
        if(score2 < qs.getBoarder()*(s.length()+1- K)) res = 1;

        sb[res].write(s.toCharArray());
        ByteCount[res] += s.length();
        sb[res].put('\n');
        ByteCount[res] += 1;
        if(res > 0) {
            sb[0].put('\n');
            ByteCount[0] += 1;
        }

        if(sb[res].size() > BUFFER_SIZE) {
            saveOneMyByteBufferToOutList(sb[res],  res, pre[res], cur[res], eline[res]);
            sb[res] = new MyByteBuffer();
            if(res==1) ++flag;
            else flag = 0;
        }
        if(flag >= 3) {
            saveOneMyByteBufferToOutList(sb[0], (byte) 0, pre[0], cur[0], eline[0]);
            sb[res] = new MyByteBuffer();
            flag = 0;
        }
    }

    public void writeOneMyByteBufferToFile(MyByteBuffer s, byte bucket, int start, int end, int eline) throws Exception {
        BlockBufInfo bf = new BlockBufInfo();
        bf.bucket=bucket;
        bf.in = s;
        assert ByteCount[bucket] == bf.in.size();
        pack(bf.in, bucket);
        try {

            bw.write(String.valueOf(bucket));
            bw.write(" ");
            bw.write(String.valueOf(bf.in.size()));
            bw.newLine();

            int tempCount = bf.in.size();
            int i = 0;
            int out = 0;
            while((out = bf.in.get())!=-1){
                i++;
                bw.write(out);
            }
            bw.newLine();
            bw.flush();

            assert tempCount == i;

        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteCount[bucket] = 0;

    }



    public void saveOneMyByteBufferToOutList(MyByteBuffer s, byte bucket, int start, int end, int eline) throws Exception {
        BlockBufInfo bf = new BlockBufInfo();
        bf.bucket=bucket;
        bf.in = s;
        if(score == 0) {
            score = '#';
        }
        pack(bf.in, bucket);
        try {
            int temp = bf.in.size();
            String key = score + " " + String.valueOf(bucket) + " " + bf.in.size();
            byte[] value = bf.in.getAll();
            assert temp == value.length;
            outList.add(new Tuple2<>(key, value));

        } catch (Exception e) {
            e.printStackTrace();
        }

        ByteCount[bucket] = 0;

    }
}
