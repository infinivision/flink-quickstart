package org.infinivision.flink.streaming.entity;

public class Trade implements Comparable<Trade> {

    public Trade() {}

    public Trade(Long timestamp, Long customerId, String tradeInfo) {

        this.timestamp = timestamp;
        this.customerId = customerId;
        this.tradeInfo = tradeInfo;
    }

    public Long timestamp;
    public Long customerId;
    public String tradeInfo;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Trade(").append(timestamp).append(") ");
        sb.append(tradeInfo);
        return sb.toString();
    }

    public int compareTo(Trade other) {
        return Long.compare(this.timestamp, other.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        else if (o != null && getClass() == o.getClass()) {
            Trade that = (Trade) o;
            return ((this.customerId.equals(that.customerId)) && (this.timestamp.equals(that.timestamp)));
        }
        return false;
    }

}