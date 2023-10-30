package com.example.hadoopprj.component.impl;

import com.example.hadoopprj.component.IHdfsExam;
import com.example.hadoopprj.dto.HadoopDTO;
import com.example.hadoopprj.service.IHdfsFileReadService;
import com.example.hadoopprj.service.IHdfsFileUploadService;
import com.example.hadoopprj.service.ILocalGzFileReadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Log4j
@Component
@RequiredArgsConstructor
public class Exam03 implements IHdfsExam {
    private final ILocalGzFileReadService localGzFileReadService;
    private final IHdfsFileUploadService hdfsFileUploadService;
    private final IHdfsFileReadService hdfsFileReadService;

    @Override
    public void doExam() throws Exception {
        HadoopDTO pDTO = null;

        log.info("[실습1] Gz로 압축된 파일 내용 중 10.56.xxx.xxx 로그만 올리기");

        pDTO = new HadoopDTO();

        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("access_log.gz");
        pDTO.setRegExp("10\\.223\\.[0-9]{1,3}\\.[0-9]{1,3}");

        List<String> ipLog = localGzFileReadService.readLocalGzFileIP(pDTO);

        pDTO = null;

        log.info("[실습1 결과] Gz로 압축된 파일 내용 중 10.56.xxx.xxx 로그만 올리기");
        log.info(ipLog);

        log.info("[실습2] HDFS에 access_log.gz 파일 중 10.56.xxx.xxx 로그만 올리기");

        pDTO = new HadoopDTO();

        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("10.56.xxx.xxx.log");
        pDTO.setContentList(ipLog);

        hdfsFileUploadService.uploadHdfsFileContents(pDTO);

        pDTO = null;

        log.info("[실습3] HDFS에 저장된 파일 내용 보기!");

        pDTO = new HadoopDTO();

        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("10.56.xxx.xxx.log");

        String hadoopLog = hdfsFileReadService.readHdfsFile(pDTO);

        log.info("[실습3 결과] HDFS에 저장된 파일 내용");
        log.info(hadoopLog);

        pDTO = null;

    }
}
