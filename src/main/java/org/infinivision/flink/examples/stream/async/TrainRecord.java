package org.infinivision.flink.examples.stream.async;

public class TrainRecord {
    private int aid;
    private int uid;
    private int label;


    public TrainRecord(int aid, int uid, int label) {
        this.aid = aid;
        this.uid = uid;
        this.label = label;
    }

    public int getAid() {
        return aid;
    }

    public void setAid(int aid) {
        this.aid = aid;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public int getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return "TrainRecord{" +
                "aid=" + aid +
                ", uid=" + uid +
                ", label=" + label +
                '}';
    }
}
