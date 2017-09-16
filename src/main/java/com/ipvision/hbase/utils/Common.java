/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbase.utils;

import java.io.FileInputStream;
import java.security.MessageDigest;

/**
 *
 * @author rakib
 */
public class Common {
    
       public static String calculateCheckSum(String file) throws Exception {

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        FileInputStream fis = new FileInputStream(file);
        byte[] block = new byte[4096];
        int length;
        while ((length = fis.read(block)) > 0) {
            md5.update(block, 0, length);
        }

        byte[] mdbytes = md5.digest();

        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < mdbytes.length; i++) {
            sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));

        }
        return sb.toString();
    }
    
}
