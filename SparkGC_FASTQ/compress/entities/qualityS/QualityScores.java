package compress.entities.qualityS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QualityScores implements java.io.Serializable {
    public static final int SAMPLE_NUM = 100;
    private List<String> sample;
    private Map<Long, Double> table;

    private double boarder;

    public QualityScores() {
        sample = new ArrayList<>(SAMPLE_NUM);//M=100000
        table = new HashMap<>(SAMPLE_NUM/100);
    }

    public void setBoarder(double boarder) {
        this.boarder = boarder;
    }

    public double getBoarder() {
        return boarder;
    }

    public Map<Long, Double> getTable() {
        return table;
    }

    public void addTableByturn(long key, double value) {
        this.table.put(key, value);
    }

    public List<String> getSample() {
        return sample;
    }

    public void addSampleByturn(String str) {
        this.sample.add(str);
    }

    public void setSampleByIndex(int num, String str) {
        this.sample.set(num, str);
    }

}
