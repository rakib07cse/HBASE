/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import com.ipvision.hbase.utils.Common;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author rakib
 */
public class LogParser {

    private static final Pattern logPattern = Pattern.compile(
            "^(\\d{17})\\s+(\\w+)\\s*-?\\s*(R|\\d+)\\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\\s+(\\w+)\\s*-?\\s*\\{{1}(.*?)\\}{1})?$");
    private static Pattern logPattern1 = Pattern.compile("^(\\d{17})\\s+(ERROR|WARN|FATAL|INFO)\\s+-?\\s*(R|((LiveStreamHistory)->(.*})|.*\\))(\\s+.*)?)(\\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\\s(\\w+)\\s*-?(.*))?$");

    public List<LogBean> sendFileForProcessing(File file) throws Exception {
        String currentLine = null;

        LogBean logBean ;
        CheckSumProcessValuePair checkSumProcessValuePair = new CheckSumProcessValuePair();

        List<LogBean> logBeanList = new ArrayList<>();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        String md5CheckSum = Common.calculateCheckSum(file.getAbsolutePath());
        String mapKey = file.getName();

        if (LogParserManager.processedLogMap.containsKey(mapKey)) {
            randomAccessFile.seek(LogParserManager.processedLogMap.get(mapKey).getProcessByte());
        }

        while ((currentLine = randomAccessFile.readLine()) != null) {
            logBean = parseMethod(currentLine);
            if (logBean != null) {
                logBeanList.add(logBean);
            }
        }

        checkSumProcessValuePair.setCheckSum(md5CheckSum);
        checkSumProcessValuePair.setProcessByte(randomAccessFile.getFilePointer());
        LogParserManager.processedLogMap
                .put(mapKey, checkSumProcessValuePair);

        return logBeanList;

    }

    public LogBean parseMethod(String currentLine) throws Exception {

        LogBean logBean = new LogBean();

        Matcher logMatcher = logPattern1.matcher(currentLine);

        int grpCount = logMatcher.groupCount();

        if (logMatcher.matches()) {

            if (grpCount >= 1) {
                logBean.setTimestamp(logMatcher.group(1));
            }
            if (grpCount >= 2) {
                logBean.setLogLevel(logMatcher.group(2));
            }
            if (grpCount >= 4) {
                logBean.setEventType(logMatcher.group(4));
            }
            if (grpCount >= 5) {
                logBean.setLiveStreamHistory(logMatcher.group(5));
            }
            if (grpCount >= 6) {
                logBean.setLiveStreamParams(logMatcher.group(6));
            }
            if (grpCount >= 9) {
                logBean.setRequestId(logMatcher.group(9));
            }
            if (grpCount >= 10) {
                logBean.setMethodName(logMatcher.group(10));
            }
            if (grpCount >= 11) {
                logBean.setParams(logMatcher.group(11));
            }
        }

        return logBean;

    }

}
