package com.ifchange.spark.bi.bean.tob;

import java.io.Serializable;

public class IdwTobBaseContact implements Serializable {

    private long resume_id;

    private String name;

    private String phone;

    private String email;

    public long getResume_id() {
        return resume_id;
    }

    public void setResume_id(long resume_id) {
        this.resume_id = resume_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
