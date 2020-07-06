package com.ifchange.spark.bi.bean.position;

import java.io.Serializable;

public class IdwPositionsIndustryTemp implements Serializable {

    private long position_id;

    private long industry_id;

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
}
