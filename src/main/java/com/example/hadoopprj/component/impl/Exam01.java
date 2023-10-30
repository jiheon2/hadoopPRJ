package com.example.hadoopprj.component.impl;

import com.example.hadoopprj.component.IHdfsExam;
import com.example.hadoopprj.dto.HadoopDTO;
import com.example.hadoopprj.service.impl.HdfsFileUploadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;

@Log4j
@RequiredArgsConstructor
@Component
public class Exam01 implements IHdfsExam {
    private final HdfsFileUploadService hdfsFileUploadService;

    @Override
    public void doExam() throws Exception {
        HadoopDTO pDTO = new HadoopDTO();

        // 하둡에 저장될 파일 정보
        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("access_log.gz");

        // 내 컴퓨터에 존재하는 업로드 할 파일 정보
        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("access_log.gz");

        hdfsFileUploadService.uploadHdfsFile(pDTO);
    }
}
/*
HDFS에 access_log.gz 파일 업로드 실습 실행을 위한 파일
 */