/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 * @author rakib
 */
public class LogParserRunner extends Thread {
    
    private static final Logger logger = LogManager.getLogger(LogParserRunner.class);
    
    private final String configFile;
    
    static final long SECOND = 1000L;
    static final long MINUTE = 60 * SECOND;
    static final long HOUR = 60 * MINUTE;
    
    public LogParserRunner(String configFile) {
        this.configFile = configFile;
        
    }
    
    @Override
    public void run() {
        while (true) {
            
            long currentTime = System.currentTimeMillis();
            long nextRunTime = (((currentTime / HOUR) + 1) * HOUR) + (60 * MINUTE);

            //***************METHOD EXTRACTOR RUNNING CODE BEGINS HERE *************************
            try {
                LogParserManager logParserManager = new LogParserManager(configFile);
                logParserManager.processLog();
                        
            } catch (Exception ex) {
                logger.warn("Error while process method.", ex);
            }
            
            long waitngTime = nextRunTime - System.currentTimeMillis();
            
            if (waitngTime > 0) {
                try {
                    sleep(waitngTime);
                } catch (InterruptedException ex) {
                    logger.error("Exception occured for Thread.sleep()", ex);
                }
            }
            
        }
    }
    
    public static void main(String[] args) {
        
        LogParserRunner runner = new LogParserRunner("config.properties");
        runner.start();
        
    }
}
