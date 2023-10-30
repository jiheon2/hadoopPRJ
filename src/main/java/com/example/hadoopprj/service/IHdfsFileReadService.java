package com.example.hadoopprj.service;

import com.example.hadoopprj.dto.HadoopDTO;

public interface IHdfsFileReadService {
    String readHdfsFile(HadoopDTO pDTO) throws Exception;
}
