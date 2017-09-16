/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import com.ipvision.hbase.utils.Common;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rakib
 */
public class MethodExtractor {

    private FileReader fileReader = null;
    private BufferedReader bufferedReader = null;
    private String currentLine = null;

    public void processFile(File file) throws Exception {

        MethodParser methodPaser = new MethodParser();
        MethodBean methodBean = new MethodBean();
        //MethodExtractorManager methodExtractorManager = new MethodExtractorManager();
        CheckSumProcessValuePair checkSumProcessValuePair = new CheckSumProcessValuePair();
        ArrayList<MethodBean> methodBeanList = new ArrayList<>();

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");

        String md5CheckSum = Common.calculateCheckSum(file.getAbsolutePath());

        String key = file.getName();
        System.out.println(key);
        if (MethodExtractorManager.getProcessedLogMap().containsKey(key)) {

            randomAccessFile.seek(MethodExtractorManager.getProcessedLogMap().get(key).getProcessByte());
        }

        while ((currentLine = randomAccessFile.readLine()) != null) {
            methodBean = methodPaser.parseMethod(currentLine);
            methodBeanList.add(methodBean);

        }
        checkSumProcessValuePair.setCheckSum(md5CheckSum);
        checkSumProcessValuePair.setProcessByte(randomAccessFile.getFilePointer());
        MethodExtractorManager.getProcessedLogMap().put(key, checkSumProcessValuePair);
        
        //HBaseManager.getHBaseManager().createHBaseTable("emp");
        System.out.println("Table create successfully");

        //fileReader = new FileReader(file);
        //bufferedReader = new BufferedReader(fileReader);
//        try {
//            while ((currentLine = bufferedReader.readLine()) != null) {
//
//                methodBean = methodPaser.parseMethod(currentLine);
//                methodBeanList.add(methodBean);
//
//            }
//        } catch (IOException ex) {
//            Logger.getLogger(MethodExtractor.class.getName()).log(Level.SEVERE, null, ex);
//        } finally {
//            try {
//                if (bufferedReader != null) {
//                    bufferedReader.close();
//                }
//                if (fileReader != null) {
//                    fileReader.close();
//                }
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            }
//        }
    }

}
