/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import java.io.Serializable;

/**
 *
 * @author rakib
 */
public class MethodBean implements Serializable {

    private String timestamp;
    private String methodName;
    private String data;

    public MethodBean() {

    }

    public MethodBean(String timestamp, String methodName, String data) {
        this.timestamp = timestamp;
        this.methodName = methodName;
        this.data = data;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getData() {
        return this.data;
    }

}
