package com.example.hadoopprj.service;

import org.apache.hadoop.conf.Configuration;

/*
 하둡 접속을 위한 공통 설정을 추상화 객체로 정의
 추상화 객체란? >
 */
public abstract class AbstractHadoopConf {
    String namenodeHost = "192.168.115.132"; // 네임노드가 설치된 마스터 서버 IP

    String namenodePort = "9000"; // 네임노드 포트

    String yarnPort = "8080"; // 얀 포트

    public Configuration getHadoopConfiguration() {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://" + namenodeHost + ":" + namenodePort);

        conf.set("yarn.resourcemanager.address", namenodeHost + ":" + yarnPort);

        return conf;
    }
}
