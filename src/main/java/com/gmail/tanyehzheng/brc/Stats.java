package com.gmail.tanyehzheng.brc;

public class Stats {

    private int count = 0;
    private double sum = 0;
    private double min = 999;
    private double max = -999;

    public void addReading(double reading) {
        this.count++;
        this.sum += reading;
        this.min = Math.min(this.min, reading);
        this.max = Math.max(this.max, reading);
    }

    public void merge(Stats other) {
        this.count += other.count;
        this.sum += other.sum;
        this.min = Math.min(this.min, other.min);
        this.max = Math.max(this.max, other.max);
    }

    @Override
    public String toString() {
        return String.format("min: %2f, avg: %2f, max: %2f", this.min, getAverage(), this.max);
    }

    public int getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getAverage() {
        return sum/count;
    }
}
