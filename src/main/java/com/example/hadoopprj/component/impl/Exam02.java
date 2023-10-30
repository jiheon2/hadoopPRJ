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

/*
    실습 내용
    1. Gz로 압축된 파일 최초 10줄만 읽기 실습
    2. HDFS에 access_log.gz 파일 중 최초 10줄 올리기
    3. HDFS에 저장된 파일 내용 보기
 */
@Log4j
@Component
@RequiredArgsConstructor
public class Exam02 implements IHdfsExam {

    private final ILocalGzFileReadService localGzFileReadService;
    private final IHdfsFileUploadService hdfsFileUploadService;
    private final IHdfsFileReadService hdfsFileReadService;
    @Override
    public void doExam() throws Exception {
        HadoopDTO pDTO = null;

        log.info(this.getClass().getName() + ".[실습1] Gz로 압축된 파일 최초 10줄만 읽기 실습!");

        pDTO = new HadoopDTO();

        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("access_log.gz");
        pDTO.setLineCnt(10);

        List<String> line10 = localGzFileReadService.readLocalGzFileCnt(pDTO);

        pDTO = null;

        log.info(".[실습1 결과] 최초 읽은 10줄 내용 : " + line10);

        log.info(".[실습2] HDFS에 access_log.gz 파일 중 최초 10줄 올리기!");

        pDTO = new HadoopDTO();

        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("line10.log");
        pDTO.setContentList(line10);

        hdfsFileUploadService.uploadHdfsFileContents(pDTO);

        pDTO = null;

        log.info(".[실습3] HDFS에 저장된 파일 내용 보기!");

        pDTO = new HadoopDTO();

        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("line10.log");

        String hadoopLog = hdfsFileReadService.readHdfsFile(pDTO);

        pDTO = null;
    }
}
