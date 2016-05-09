package com.zmyuan.hdfs.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by zhudebin on 16/4/28.
 * 通过client api 访问ha的hdfs
 */
public class HaAccessor {

    public static void main(String[] args) {
        HaAccessor haAccessor = new HaAccessor();
        haAccessor.testRead();
    }

    /**
     * 测试读取
     */
    public void testRead() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ehadoop");
        conf.set("dfs.nameservices", "ehadoop");
        conf.set("dfs.ha.namenodes.ehadoop", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.ehadoop.nn1", "192.168.137.101:8020");
        conf.set("dfs.namenode.rpc-address.ehadoop.nn2", "192.168.137.102:8020");
        conf.set("dfs.client.failover.proxy.provider.ehadoop", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            FileStatus[] list = fs.listStatus(new Path("/TestData"));
            for (FileStatus file : list) {
                System.out.println(file.getPath().getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
