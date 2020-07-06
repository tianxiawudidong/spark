package com.ifchange.spark.bi.bean.cv;

import java.io.Serializable;

public class IndustryMapping implements Serializable {

    private Integer id;

    private Integer pindustry_id;

    private Integer depth;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getPindustry_id() {
        return pindustry_id;
    }

    public void setPindustry_id(Integer pindustry_id) {
        this.pindustry_id = pindustry_id;
    }

    public Integer getDepth() {
        return depth;
    }

    public void setDepth(Integer depth) {
        this.depth = depth;
    }
}
