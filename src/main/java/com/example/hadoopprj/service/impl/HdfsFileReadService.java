package com.example.hadoopprj.service.impl;

import com.example.hadoopprj.dto.HadoopDTO;
import com.example.hadoopprj.service.AbstractHadoopConf;
import com.example.hadoopprj.service.IHdfsFileReadService;
import com.example.hadoopprj.util.CmmUtil;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

@Log4j
@Service
public class HdfsFileReadService extends AbstractHadoopConf implements IHdfsFileReadService {
    @Override
    public String readHdfsFile(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".readHdfsFile Start!");

        FileSystem hdfs = FileSystem.get(this.getHadoopConfiguration()); // 하둡 분산 파일 시스템 객체 생성

        String hadoopFile = CmmUtil.nvl(pDTO.getHadoopUploadPath()) + "/" + CmmUtil.nvl(pDTO.getHadoopUploadFileName());
        // 하둡분산파일시스템에 저장될 파일 경로 및 파일명

        Path path = new Path(hadoopFile); // 하둡분산파일시스템에 저장가능한 객체로 변환

        log.info("HDFS Exists : " + hdfs.exists(path)); // 로그로 값 확인

        String readLog = ""; // 로그 생성

        if(hdfs.exists(path)) { // 파일이 존재한다면
            FSDataInputStream inputStream = hdfs.open(path);
            readLog = inputStream.readUTF();
            inputStream.close();
            // 하둡분산파일시스템의 파일 읽기
            // FSDataInputStream 객체는 ByteBufferReadable를 상속하여 구현
            // 파일 업로드 및 다운로드, 읽기에서는 buffer를 사용해서 구현해라
        }

        log.info(this.getClass().getName() + ".readHdfsFile End!");

        return readLog;
    }
}
