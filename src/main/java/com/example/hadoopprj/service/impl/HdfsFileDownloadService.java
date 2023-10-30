package com.example.hadoopprj.service.impl;

import com.example.hadoopprj.dto.HadoopDTO;
import com.example.hadoopprj.service.AbstractHadoopConf;
import com.example.hadoopprj.service.IHdfsFileDownloadService;
import com.example.hadoopprj.util.CmmUtil;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

@Log4j
@Service
public class HdfsFileDownloadService extends AbstractHadoopConf implements IHdfsFileDownloadService {
    @Override
    public void downloadHdfsFile(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".downloadFile Start!");

        FileSystem hdfs = FileSystem.get(this.getHadoopConfiguration()); // CentOS에 설치된 하둡 분산 파일시스템 연결 및 설정

        String hadoopUploadFilePath = CmmUtil.nvl(pDTO.getHadoopUploadPath()); // 다운로드 하기위한 하둡에 저장된 폴더

        String hadoopUploadFileName = CmmUtil.nvl(pDTO.getHadoopUploadFileName()); // 다운로드 하기 위한 하둡에 저장된 파일 명

        String hadoopFile = hadoopUploadFilePath + "/" + hadoopUploadFileName; // 하둡분산파일시스템에 저장될 파일경로 및 폴더명

        Path path = new Path(hadoopFile); // 하둡분산파일시스템에 저장가능한 객체로 변환

        log.info("HDFS Exists : " + hdfs.exists(path)); // 로그로 확인

        if(hdfs.exists(path)) { // 하둡에 파일이 존재하면..
            Path localPath = new Path(CmmUtil.nvl(pDTO.getLocalUploadPath()) + "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName())); // 다운로드 파일 명

            hdfs.copyToLocalFile(path,localPath); // 업로드 된 파일 다운
        }
        hdfs.close();

        log.info(this.getClass().getName() + ".downloadFile End!");
    }
}
