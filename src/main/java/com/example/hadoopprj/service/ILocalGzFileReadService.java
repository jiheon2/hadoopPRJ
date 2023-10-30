package com.example.hadoopprj.service;

import com.example.hadoopprj.dto.HadoopDTO;

import java.util.List;

public interface ILocalGzFileReadService {
    List<String> readLocalGzFileCnt(HadoopDTO pDTO) throws Exception; // Gz파일의 최초 기준 라인수 만큼 읽기

    List<String> readLocalGzFileIP(HadoopDTO pDTO) throws Exception; // Gz파일의 IP대역 읽기
}
