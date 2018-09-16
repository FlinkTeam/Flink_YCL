package com.run.ycl.parser.file;

import java.util.Arrays;

public class IndexDesBean {
    /**
     * 协议名称
     */
    private String dDataset;

    private int dataBeginLineNum;

    private String fileName;

    private String sDataset;

    private String[] items;

    public String[] getItems() {
        return items;
    }

    public void setItems(String[] items) {
        this.items = items;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getdDataset() {
        return dDataset;
    }

    public void setdDataset(String dDataset) {
        this.dDataset = dDataset;
    }

    public void setsDataset(String sDataset) {
        this.sDataset = sDataset;
    }

    public String getsDataset() {
        return sDataset;
    }

    public int getDataBeginLineNum() {
        return dataBeginLineNum;
    }

    public void setDataBeginLineNum(int dataBeginLineNum) {
        this.dataBeginLineNum = dataBeginLineNum;
    } @Override
    public String toString() {
        return "IndexDesBean{" +
                "dDataset='" + dDataset + '\'' +
                ", dataBeginLineNum=" + dataBeginLineNum +
                ", fileName='" + fileName + '\'' +
                ", sDataset='" + sDataset + '\'' +
                ", items=" + Arrays.toString(items) +
                '}';
    }


}
