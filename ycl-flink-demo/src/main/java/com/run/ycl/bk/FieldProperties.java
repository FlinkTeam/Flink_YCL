package com.run.ycl.bk;

import java.io.Serializable;

public class FieldProperties implements Serializable {
    String chname;
    String enname;
    String elemenId;
    String beNotNull;
    String valueLength;
    String valueType;

    public void setBeNotNull(String beNotNull) {
        this.beNotNull = beNotNull;
    }

    public void setChname(String chname) {
        this.chname = chname;
    }

    public void setElemenId(String elemenId) {
        this.elemenId = elemenId;
    }

    public void setEnname(String enname) {
        this.enname = enname;
    }

    public void setValueLength(String valueLength) {
        this.valueLength = valueLength;
    }

    public String getBeNotNull() {
        return beNotNull;
    }

    public String getChname() {
        return chname;
    }

    public String getElemenId() {
        return elemenId;
    }

    public String getEnname() {
        return enname;
    }

    public String getValueLength() {
        return valueLength;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    @Override
    public String toString() {
        return getElemenId();
    }
}
