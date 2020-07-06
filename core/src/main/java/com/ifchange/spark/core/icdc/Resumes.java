package com.ifchange.spark.core.icdc;

import java.io.Serializable;

public class Resumes implements Serializable {
    private long resumeId;
    private long contactId;

    public long getResumeId() {
        return resumeId;
    }

    public void setResumeId(long resumeId) {
        this.resumeId = resumeId;
    }

    public long getContactId() {
        return contactId;
    }

    public void setContactId(long contactId) {
        this.contactId = contactId;
    }
}