package com.ifchange.spark.bi.bean.position;

import java.io.Serializable;

public class IdwPositionsIndustry implements Serializable {

    private long position_id;

    private long industry_id;

    private int depth;

    public long getPosition_id() {
        return position_id;
    }

    public void setPosition_id(long position_id) {
        this.position_id = position_id;
    }

    public long getIndustry_id() {
        return industry_id;
    }

    public void setIndustry_id(long industry_id) {
        this.industry_id = industry_id;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }
}
