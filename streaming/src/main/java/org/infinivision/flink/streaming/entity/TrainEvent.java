package org.infinivision.flink.streaming.entity;

public class TrainEvent {
    private String aid;
    private String uid;
    private int label;
    private long timestamp;

    public TrainEvent() {}

    public TrainEvent(String aid, String uid, int label, long timestamp) {
        this.aid = aid;
        this.uid = uid;
        this.label = label;
        this.timestamp = timestamp;
    }

    public String getAid() {
        return aid;
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public static TrainEvent fromString(String eventStr) {
        String[] split = eventStr.split(",");
        return new TrainEvent(split[0], split[1], Integer.valueOf(split[2]), Long.valueOf(split[3]));
    }

    @Override
    public String toString() {
        return aid + "," + uid + "," + label + "," + timestamp;
    }
}
