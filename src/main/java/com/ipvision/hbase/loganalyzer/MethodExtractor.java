/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import com.ipvision.hbase.db.HBaseInserter;
import com.ipvision.hbase.utils.Common;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author rakib
 */
public class MethodExtractor {

    private static final Logger methodExtractorLogger = Logger.getLogger(MethodExtractor.class.getName());

    public void processFile(File file) throws Exception {

        methodExtractorLogger.entering(getClass().getName(), "entering processFile method");
        String currentLine = null;

        MethodParser methodPaser = new MethodParser();
        MethodBean methodBean = new MethodBean();
        CheckSumProcessValuePair checkSumProcessValuePair = new CheckSumProcessValuePair();

        List<MethodBean> methodBeanList = new ArrayList<>();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        String md5CheckSum = Common.calculateCheckSum(file.getAbsolutePath());
        String mapKey = file.getName();

        if (MethodExtractorManager.getProcessedLogMap()
                .containsKey(mapKey)) {

            randomAccessFile.seek(MethodExtractorManager.getProcessedLogMap().get(mapKey).getProcessByte());
        }

        while ((currentLine = randomAccessFile.readLine()) != null) {
            methodBean = methodPaser.parseMethod(currentLine);
            methodBeanList.add(methodBean);

        }

        HBaseInserter.addMethod(methodBeanList);

        checkSumProcessValuePair.setCheckSum(md5CheckSum);

        checkSumProcessValuePair.setProcessByte(randomAccessFile.getFilePointer());
        MethodExtractorManager.getProcessedLogMap()
                .put(mapKey, checkSumProcessValuePair);

        methodExtractorLogger.exiting(getClass().getName(), "Exiting processFile method");
    }

}
