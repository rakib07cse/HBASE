/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import com.ipvision.hbase.utils.Common;
import com.ipvision.hbase.utils.Tools;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 * @author rakib
 */
public class MethodExtractorManager {

    private static final Logger logger = LogManager.getLogger(MethodExtractorManager.class);

    private String processingLogDirectory;
    private String processedLogMapFile;
    private ArrayList<File> files = new ArrayList<>();
    private final Pattern directoryPattern = Pattern.compile("[\\d]{4}-[\\d]{2}");

    private static HashMap<String, CheckSumProcessValuePair<String, Long>> processedLogMap;

    MethodExtractorManager(String configFile) throws Exception {
        Properties properties = loadProperties(configFile);
        getConfiguration(properties);
        loadProcessedLogMap(processedLogMapFile);
    }

    public void processLogMethod() throws Exception {
        MethodExtractor methodExtractor = new MethodExtractor();

        File processingLogDir = new File(this.processingLogDirectory);
        addLogFiles(processingLogDir);
        for (File file : files) {
            System.out.println(file);
            methodExtractor.processFile(file);
        }

        File mapFile = new File(processedLogMapFile);
        try (FileOutputStream fileOut = new FileOutputStream(mapFile); ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(processedLogMap);
            out.flush();
        }

        processedLogMap.keySet().stream().forEach((key) -> {
            System.out.println(key + " " + processedLogMap.get(key).getCheckSum() + " " + processedLogMap.get(key).getProcessByte());
        });

    }

    private Properties loadProperties(String configFile) throws Exception {

        InputStream input = null;
        try {
            File file = new File(configFile);
            if (file.exists()) {
                input = new FileInputStream(file);
            } else {
                input = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
            }
            Properties properties = new Properties();
            properties.load(input);
            return properties;
        } catch (Exception ex) {
            String msg = String.format("Exception occured while loading configuration file \"%s\"", configFile);
            throw new Exception(msg, ex);
        } finally {
            if (null != input) {
                input.close();
            }
        }

    }

    private void getConfiguration(Properties properties) {

        if (!properties.containsKey(Tools.LOG_DIR_KEY)) {
            throw new IllegalArgumentException("\"" + Tools.LOG_DIR_KEY + "\" is not found in configuration file. ");
        }

        String logDirectoryPath = properties.getProperty(Tools.LOG_DIR_KEY);
        File logDirectory = new File(logDirectoryPath);

        if (!logDirectory.isDirectory()) {
            throw new IllegalArgumentException("\"" + logDirectory.getAbsolutePath() + "\" is not a directory.");
        }

        this.processingLogDirectory = logDirectory.getAbsolutePath();

        if (!properties.containsKey(Tools.PROCESSED_LOG_MAP)) {
            throw new IllegalArgumentException("\"" + Tools.PROCESSED_LOG_MAP + "\" is not found in configuration file.");
        }
        String filePath = properties.getProperty(Tools.PROCESSED_LOG_MAP);
        File processedLogFilePath = new File(filePath);
        if (!processedLogFilePath.isFile()) {
            throw new IllegalArgumentException("\"" + Tools.PROCESSED_LOG_MAP + "\" is not a file.");
        }
        if (!processedLogFilePath.canRead()) {
            throw new IllegalArgumentException("\"" + Tools.PROCESSED_LOG_MAP + "\" is not read able file.");
        }

        this.processedLogMapFile = processedLogFilePath.getAbsolutePath();

    }

    private void loadProcessedLogMap(String processedLogMapFile) throws Exception {

        File file = new File(processedLogMapFile);
        if (file.exists()) {
            FileInputStream fileIn = new FileInputStream(file);

            try {
                try (ObjectInputStream in = new ObjectInputStream(fileIn)) {
                    processedLogMap = (HashMap<String, CheckSumProcessValuePair<String, Long>>) in.readObject();
                }
            } catch (EOFException ex) {
                processedLogMap = new HashMap<>();
            }
        }
        processedLogMap.keySet().stream().forEach((key) -> {
            System.out.println(key + ":" + processedLogMap.get(key));
        });
    }

    private void addLogFiles(File processingLogDir) throws Exception {
        String md5Checksum = null;
        String mapKey = null;
        
        for (File file : processingLogDir.listFiles()) {
            if (file.isDirectory()) {
                addLogFiles(file);
            }
            if (file.isFile()) {
                mapKey = file.getName();
                md5Checksum = Common.calculateCheckSum(file.getAbsolutePath());
                if (!processedLogMap.containsKey(mapKey)) {
                    files.add(file);
                } else if (!md5Checksum.equals(processedLogMap.get(mapKey).getCheckSum())) {
                    files.add(file);
                }
            }

        }

    }

    public static HashMap<String, CheckSumProcessValuePair<String, Long>> getProcessedLogMap() {
        return processedLogMap;
    }

//    private String calculateCheckSum(String file) throws Exception {
//
//        MessageDigest md5 = MessageDigest.getInstance("MD5");
//        FileInputStream fis = new FileInputStream(file);
//        byte[] block = new byte[4096];
//        int length;
//        while ((length = fis.read(block)) > 0) {
//            md5.update(block, 0, length);
//        }
//
//        byte[] mdbytes = md5.digest();
//
//        StringBuilder sb = new StringBuilder("");
//        for (int i = 0; i < mdbytes.length; i++) {
//            sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
//
//        }
//        return sb.toString();
//    }
}
