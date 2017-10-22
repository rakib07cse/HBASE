/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.loganalyzer;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author rakib
 */
public interface Analyzer extends Closeable {

    public void processLog(List<LogBean> methodBeanList);

    public void saveToDB() throws SQLException;
}