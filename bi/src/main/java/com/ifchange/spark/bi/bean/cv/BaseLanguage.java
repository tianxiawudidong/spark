package com.ifchange.spark.bi.bean.cv;

import java.io.Serializable;

public class BaseLanguage implements Serializable {

    private long resume_id;

    private String lid;

    private String certificate;

    private String name;

    private String level;

    private String created_at;

    private String updated_at;

    private int sort_id;

    public long getResume_id() {
        return resume_id;
    }

    public void setResume_id(long resume_id) {
        this.resume_id = resume_id;
    }

    public String getLid() {
        return lid;
    }

    public void setLid(String lid) {
        this.lid = lid;
    }

    public String getCertificate() {
        return certificate;
    }

    public void setCertificate(String certificate) {
        this.certificate = certificate;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }

    public int getSort_id() {
        return sort_id;
    }

    public void setSort_id(int sort_id) {
        this.sort_id = sort_id;
    }
}
