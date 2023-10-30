package com.example.hadoopprj.service;

import com.example.hadoopprj.dto.HadoopDTO;

public interface IHdfsFileDownloadService {
    void downloadHdfsFile(HadoopDTO pDTO) throws Exception;
}
