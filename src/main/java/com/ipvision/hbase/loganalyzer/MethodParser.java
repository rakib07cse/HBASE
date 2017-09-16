/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author rakib
 */
public class MethodParser {

    private static final Pattern logPattern = Pattern.compile(
            "^(\\d{17})\\s+(\\w+)\\s*-?\\s*(R|\\d+)\\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\\s+(\\w+)\\s*-?\\s*\\{{1}(.*?)\\}{1})?$");

    public MethodBean parseMethod(String currentLine) throws Exception {
        
        MethodBean methodBean = new MethodBean();
        
        Matcher methodMatcher = logPattern.matcher(currentLine);
        
        int grpCount = methodMatcher.groupCount();
        
        if(methodMatcher.matches()){
            if(grpCount >= 1){
                methodBean.setTimestamp(methodMatcher.group(1));
            }
            
            if(grpCount>=6){
                methodBean.setMethodName(methodMatcher.group(6));
            }
        }
        
        return methodBean;

    }

}
