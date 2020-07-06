package com.ifchange.spark.bi.bean.position;

import java.io.Serializable;

public class IdwPositionsFunctions implements Serializable {

    private long position_id;

    private long function_id;

    private int depth;

    private double rank;

    public long getPosition_id() {
        return position_id;
    }

    public void setPosition_id(long position_id) {
        this.position_id = position_id;
    }

    public long getFunction_id() {
        return function_id;
    }

    public void setFunction_id(long function_id) {
        this.function_id = function_id;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }
}
