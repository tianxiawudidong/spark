package com.ifchange.spark.bi.bean.position.incre;


import java.io.Serializable;
import java.util.List;

public class IdwPositionsDataIncre implements Serializable {

    private IdwPositionsIncre idwPositionsIncre;

    private List<IdwPositionsFunctionsIncre> idwPositionsFunctionsIncreList;

    private List<IdwPositionsIndustryTempIncre> idwPositionsIndustryTempIncreList;

    private List<IdwPositionsRefreshedInfoIncre> idwPositionsRefreshedInfoIncreList;

    private IdwPositionsWorkCityIncre idwPositionsWorkCityIncre;

    public IdwPositionsIncre getIdwPositionsIncre() {
        return idwPositionsIncre;
    }

    public void setIdwPositionsIncre(IdwPositionsIncre idwPositionsIncre) {
        this.idwPositionsIncre = idwPositionsIncre;
    }

    public List<IdwPositionsFunctionsIncre> getIdwPositionsFunctionsIncreList() {
        return idwPositionsFunctionsIncreList;
    }

    public void setIdwPositionsFunctionsIncreList(List<IdwPositionsFunctionsIncre> idwPositionsFunctionsIncreList) {
        this.idwPositionsFunctionsIncreList = idwPositionsFunctionsIncreList;
    }

    public List<IdwPositionsIndustryTempIncre> getIdwPositionsIndustryTempIncreList() {
        return idwPositionsIndustryTempIncreList;
    }

    public void setIdwPositionsIndustryTempIncreList(List<IdwPositionsIndustryTempIncre> idwPositionsIndustryTempIncreList) {
        this.idwPositionsIndustryTempIncreList = idwPositionsIndustryTempIncreList;
    }

    public List<IdwPositionsRefreshedInfoIncre> getIdwPositionsRefreshedInfoIncreList() {
        return idwPositionsRefreshedInfoIncreList;
    }

    public void setIdwPositionsRefreshedInfoIncreList(List<IdwPositionsRefreshedInfoIncre> idwPositionsRefreshedInfoIncreList) {
        this.idwPositionsRefreshedInfoIncreList = idwPositionsRefreshedInfoIncreList;
    }

    public IdwPositionsWorkCityIncre getIdwPositionsWorkCityIncre() {
        return idwPositionsWorkCityIncre;
    }

    public void setIdwPositionsWorkCityIncre(IdwPositionsWorkCityIncre idwPositionsWorkCityIncre) {
        this.idwPositionsWorkCityIncre = idwPositionsWorkCityIncre;
    }
}
