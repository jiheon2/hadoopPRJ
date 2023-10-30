package com.example.hadoopprj.service;

import com.example.hadoopprj.dto.HadoopDTO;

public interface IHdfsFileUploadService {
    void uploadHdfsFile(HadoopDTO pDTO) throws Exception; // HDFS에 파일 업로드

    void uploadHdfsFileContents(HadoopDTO pDTO) throws Exception; // HDFS에 파일 일부내용만 업로드
}
