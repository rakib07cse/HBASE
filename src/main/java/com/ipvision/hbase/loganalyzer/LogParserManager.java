/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import com.ipvision.hbase.db.HBaseWriter;
import com.ipvision.hbase.utils.Common;
import com.ipvision.hbase.utils.Tools;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author rakib
 */
public class LogParserManager {

    private static final Logger methodExtractorManagerlogger = LoggerFactory.getLogger(LogParserManager.class);

    private String directoryTobeProcessed;
    private String processedLogMapFile;
    private List<File> filesTobeProcessed = new ArrayList<>();
    private final Pattern directoryPattern = Pattern.compile("[\\d]{4}-[\\d]{2}");

    public static HashMap<String, CheckSumProcessValuePair<String, Long>> processedLogMap;

    LogParserManager(String configFile) throws Exception {
        Properties properties = loadProperties(configFile);
        getConfiguration(properties);
        loadProcessedLogMap(processedLogMapFile);
    }

    public void processLog() throws Exception {
        LogParser logParser = new LogParser();
        List<LogBean> listlogBean = null;
        File logDir = new File(this.directoryTobeProcessed);
        addLogFilesToList(logDir);
        for (File file : filesTobeProcessed) {
            listlogBean = logParser.sendFileForProcessing(file);
            for (LogBean logBean : listlogBean) {
                if (logBean.getLiveStreamHistory() == null) {
                    System.out.println("LogLevel: " + logBean.getLogLevel() + "Log Type: " + logBean.getEventType() + " Method: " + logBean.getMethodName() + " LiveStrem: " + logBean.getLiveStreamHistory() + " LiveParam: " + logBean.getLiveStreamParams() + "Data: " + logBean.getParams());
                }
            }
            HBaseWriter.insertIntoHBase(listlogBean);

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

        this.directoryTobeProcessed = logDirectory.getAbsolutePath();

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

        System.out.println("File location:" + file);
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

    }

    private void addLogFilesToList(File processingLogDir) throws Exception {

        for (File file : processingLogDir.listFiles()) {
            if (file.isDirectory()) {

                String dirName = file.getName();
                if (dirName.matches("[\\d]{4}-[\\d]{2}")) {
                    System.out.println(file);
                    addLogFilesToList(file);
                } else {
                    String msg = "'" + dirName + "' is invalid sub directory in log directory.";
                    methodExtractorManagerlogger.warn(msg);
                }

            }
            if (file.isFile() && file.canRead()) {

                String mapKey = file.getName();
                String md5Checksum = Common.calculateCheckSum(file.getAbsolutePath());
                if (!processedLogMap.containsKey(mapKey)) {
                    filesTobeProcessed.add(file);
                } else if (!md5Checksum.equals(processedLogMap.get(mapKey).getCheckSum())) {
                    filesTobeProcessed.add(file);
                }
            } else {
                String msg = "'" + file.getName() + "' is not valid file for processing.";
                methodExtractorManagerlogger.warn(msg);
            }

        }

    }

}
