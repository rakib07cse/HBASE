/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.db;

import com.ipvision.hbase.loganalyzer.LogBean;
import com.ipvision.hbase.utils.Tools;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author rakib
 */
public class HBaseWriter {

    private static final Logger hBaseInserterLogger = LoggerFactory.getLogger(HBaseWriter.class);
    private static final SimpleDateFormat tableNameFormat = new SimpleDateFormat("yyyyMMddHH");

    public static void insertIntoHBase(List<LogBean> logBeanList) throws IOException {
        long timeStamp = System.currentTimeMillis();
        String tableName = tableNameFormat.format(timeStamp) + "_tmp";

        if (!HBaseManager.getHBaseManager().getAdmin().tableExists(tableName)) {
            HBaseManager.getHBaseManager().createHBaseTable(tableName);
        }
        try (HTable logHTable = HBaseManager.getHBaseManager().createHTable(tableName)) {
            List<Put> logBeanPutList = new ArrayList<>();
            for (LogBean logBean : logBeanList) {

                if (logBean.getTimestamp() != null) {

                    Put logPut = HBaseManager.getHBaseManager().createLoggerPut(logBean.getTimestamp());
                    if (logBean.getMethodName() != null) {
                        logPut.add(Bytes.toBytes(Tools.HBASE_TABLE_METHOD_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_FIRST_COLUME_NAME), Bytes.toBytes(logBean.getMethodName()));
                    }
                    if (logBean.getParams() != null) {
                        logPut.add(Bytes.toBytes(Tools.HBASE_TABLE_METHOD_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_SECOND_COLUME_NAME), Bytes.toBytes(logBean.getParams()));
                    }
                    if (logBean.getLiveStreamHistory() != null) {
                        logPut.add(Bytes.toBytes(Tools.HBASE_TABLE_LIVESTREAMHISTORY_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_LIVESTREAMHISTORY_FIRST_COLUME_NAME), Bytes.toBytes(logBean.getLiveStreamHistory()));
                    }
                    if (logBean.getLiveStreamParams() != null) {
                        logPut.add(Bytes.toBytes(Tools.HBASE_TABLE_LIVESTREAMHISTORY_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_LIVESTREAMHISTORY_SECOND_COLUME_NAME), Bytes.toBytes(logBean.getLiveStreamParams()));
                    }
                    if (logBean.getLogLevel() != null) {
                        logPut.add(Bytes.toBytes(Tools.HBASE_TABLE_NOTINFO_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_NOTINOF_FIRST_COLUME_NAME), Bytes.toBytes(logBean.getLogLevel()));
                    }
                    if (logBean.getEventType() != null) {
                        logPut.add(Bytes.toBytes(Tools.HBASE_TABLE_NOTINFO_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_NOTINFO_SECOND_COLUME_NAME), Bytes.toBytes(logBean.getEventType()));
                    }

                    logBeanPutList.add(logPut);

                }
            }

            logHTable.put(logBeanPutList);

            if (logHTable != null) {
                logHTable.flushCommits();
            }
            String msg = "Data insert successfully";
            hBaseInserterLogger.info(msg);
        } catch (Exception ex) {
            String msg = "";
            hBaseInserterLogger.error(msg);
        }

    }

}
