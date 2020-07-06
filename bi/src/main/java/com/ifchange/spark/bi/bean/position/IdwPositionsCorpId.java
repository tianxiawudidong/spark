package com.ifchange.spark.bi.bean.position;

import java.io.Serializable;

public class IdwPositionsCorpId implements Serializable {

    private long position_id;

    private long origin_corp_id;

    private long real_corp_id;

    private long corp_id;

    public long getPosition_id() {
        return position_id;
    }

    public void setPosition_id(long position_id) {
        this.position_id = position_id;
    }

    public long getOrigin_corp_id() {
        return origin_corp_id;
    }

    public void setOrigin_corp_id(long origin_corp_id) {
        this.origin_corp_id = origin_corp_id;
    }

    public long getReal_corp_id() {
        return real_corp_id;
    }

    public void setReal_corp_id(long real_corp_id) {
        this.real_corp_id = real_corp_id;
    }

    public long getCorp_id() {
        return corp_id;
    }

    public void setCorp_id(long corp_id) {
        this.corp_id = corp_id;
    }
}
