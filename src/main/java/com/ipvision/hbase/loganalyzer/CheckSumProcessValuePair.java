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
class CheckSumProcessValuePair<String, Long> implements Serializable {
    
    private String checksumValue = null;
    private Long processByte = null;
    
    public CheckSumProcessValuePair(){
    
    }
    
    public CheckSumProcessValuePair(String checksumValue,Long processByte){
        this.checksumValue = checksumValue;
        this.processByte = processByte;
    }
    
    public void setCheckSum(String checkSumValue){
        this.checksumValue = checkSumValue;
    }
    
    public String getCheckSum(){
        return checksumValue;
    }
    
    public void setProcessByte(Long processByte){
        this.processByte = processByte;
    }
    
    public Long getProcessByte(){
        return processByte;
    }
}
