/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

/**
 *
 * @author rakib
 */
public class MethodBean {

    private String timestamp;
    private String methodName;

    public MethodBean() {

    }

    public MethodBean(String timestamp, String methodName) {
        this.timestamp = timestamp;
        this.methodName = methodName;
    }
    
    public void setTimestamp(String timestamp){
        this.timestamp = timestamp;
    }
    
    public String getTimeString(){
        return timestamp;
    }
    
    public void setMethodName(String methodName){
        this.methodName = methodName;
    }
    
    public String getMethodName(){
        return methodName;
    }

}
