/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.db;

import com.ipvision.hbase.loganalyzer.MethodBean;
import com.ipvision.hbase.utils.Tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author rakib
 */
public class HBaseInserter {

    public static void addMethod(List<MethodBean> methodBeanList) throws IOException {

        try (HTable methodHTable = HBaseManager.getHBaseManager().createHTable(Tools.HBASE_INSERT_TABLE_NAME)) {
            List<Put> methodNamePutList = new ArrayList<>();
            for (MethodBean methodBean : methodBeanList) {
                if (methodBean.getTimestamp() != null && methodBean.getMethodName() != null) {
                   Put methodNamePut = HBaseManager.getHBaseManager().createLoggerPut(methodBean.getTimestamp());
                    
                    methodNamePut.add(Bytes.toBytes("method"), Bytes.toBytes("method_name"), Bytes.toBytes(methodBean.getMethodName()));
                    if (methodBean.getData() != null) {
                        methodNamePut.add(Bytes.toBytes("method"), Bytes.toBytes("method_data"), Bytes.toBytes(methodBean.getData()));
                    }
                    methodNamePutList.add(methodNamePut);
                }

            }
            methodHTable.put(methodNamePutList);
            if (methodHTable != null) {
                methodHTable.flushCommits();
            }
            System.out.println("Data inserted");
        }

    }

}
