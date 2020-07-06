package com.ifchange.spark.bi.bean.tob;

import java.io.Serializable;

public class IdwTobIcdcMaps implements Serializable {

    private long tob_resume_id;

    private long icdc_resume_id;


    public long getTob_resume_id() {
        return tob_resume_id;
    }

    public void setTob_resume_id(long tob_resume_id) {
        this.tob_resume_id = tob_resume_id;
    }

    public long getIcdc_resume_id() {
        return icdc_resume_id;
    }

    public void setIcdc_resume_id(long icdc_resume_id) {
        this.icdc_resume_id = icdc_resume_id;
    }
}
