package com.ifchange.spark.bi.bean.position;

import java.io.Serializable;
import java.util.List;

public class IdwPositionsData implements Serializable {

    private IdwPositions idwPositions;

    private List<IdwPositionsFunctions> idwPositionsFunctionsList;

    private List<IdwPositionsIndustryTemp> idwPositionsIndustryTempList;

    private List<IdwPositionsRefreshedInfo> idwPositionsRefreshedInfoList;

    private IdwPositionsWorkCity idwPositionsWorkCity;

    public IdwPositions getIdwPositions() {
        return idwPositions;
    }

    public void setIdwPositions(IdwPositions idwPositions) {
        this.idwPositions = idwPositions;
    }

    public List<IdwPositionsFunctions> getIdwPositionsFunctionsList() {
        return idwPositionsFunctionsList;
    }

    public void setIdwPositionsFunctionsList(List<IdwPositionsFunctions> idwPositionsFunctionsList) {
        this.idwPositionsFunctionsList = idwPositionsFunctionsList;
    }

    public List<IdwPositionsIndustryTemp> getIdwPositionsIndustryTempList() {
        return idwPositionsIndustryTempList;
    }

    public void setIdwPositionsIndustryTempList(List<IdwPositionsIndustryTemp> idwPositionsIndustryTempList) {
        this.idwPositionsIndustryTempList = idwPositionsIndustryTempList;
    }

    public List<IdwPositionsRefreshedInfo> getIdwPositionsRefreshedInfoList() {
        return idwPositionsRefreshedInfoList;
    }

    public void setIdwPositionsRefreshedInfoList(List<IdwPositionsRefreshedInfo> idwPositionsRefreshedInfoList) {
        this.idwPositionsRefreshedInfoList = idwPositionsRefreshedInfoList;
    }

    public IdwPositionsWorkCity getIdwPositionsWorkCity() {
        return idwPositionsWorkCity;
    }

    public void setIdwPositionsWorkCity(IdwPositionsWorkCity idwPositionsWorkCity) {
        this.idwPositionsWorkCity = idwPositionsWorkCity;
    }
}
