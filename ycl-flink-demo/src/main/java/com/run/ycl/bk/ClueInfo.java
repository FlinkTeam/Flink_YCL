package com.run.ycl.bk;

import java.util.List;

public class ClueInfo {


    String clueId;
    String dataSet;

    List<Action> actions;
    List<List<String>> subClueInfo;


    private static class Action{

        public String getActionType() {
            return actionType;
        }

        public void setActionType(String actionType) {
            this.actionType = actionType;
        }

        String actionType;
        String des;
        String fillvalue;
        String operation;

        public String getDes() {
            return des;
        }

        public void setDes(String des) {
            this.des = des;
        }

        public String getFillvalue() {
            return fillvalue;
        }

        public void setFillvalue(String fillvalue) {
            this.fillvalue = fillvalue;
        }


        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "actionType='" + actionType + '\'' +
                    ", des='" + des + '\'' +
                    ", fillvalue='" + fillvalue + '\'' +
                    ", operation='" + operation + '\'' +
                    '}';
        }
    }


    public String getClueId() {
        return clueId;
    }

    public void setClueId(String clueId) {
        this.clueId = clueId;
    }

    public String getDataSet() {
        return dataSet;
    }

    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
    }

    public List<Action> getActions() {
        return actions;
    }

    public void setActions(List<Action> actions) {
        this.actions = actions;
    }

    public List< List<String>> getSubClueInfo() {
        return subClueInfo;
    }

    public void setSubClueInfo(List<List<String>> subClueInfo) {
        this.subClueInfo = subClueInfo;
    }

    @Override
    public String toString() {
        return "ClueInfo{" +
                "clueId='" + clueId + '\'' +
                ", dataSet='" + dataSet + '\'' +
                ", actions=" + actions +
                ", subClueInfo=" + subClueInfo +
                '}';
    }
}
