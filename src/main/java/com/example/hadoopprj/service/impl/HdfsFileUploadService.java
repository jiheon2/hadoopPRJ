package com.example.hadoopprj.service.impl;

import com.example.hadoopprj.dto.HadoopDTO;
import com.example.hadoopprj.service.AbstractHadoopConf;
import com.example.hadoopprj.service.IHdfsFileUploadService;
import com.example.hadoopprj.util.CmmUtil;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Log4j
@Service
public class HdfsFileUploadService extends AbstractHadoopConf implements IHdfsFileUploadService {
    @Override
    public void uploadHdfsFile(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".uploadHdfsFile Start!");

        FileSystem hdfs = FileSystem.get(this.getHadoopConfiguration()); // 하둡분산파일시스템 연결 및 설정

        String hadoopUploadFilePath = CmmUtil.nvl(pDTO.getHadoopUploadPath()); // 하둡에 업로드 할 폴더

        String hadoopUploadFileName = CmmUtil.nvl(pDTO.getHadoopUploadFileName()); // 하둡에 업로드할 파일명

        hdfs.mkdirs(new Path(hadoopUploadFilePath)); // 하둡에 폴더 생성하기

        String hadoopFile = hadoopUploadFilePath + "/" + hadoopUploadFileName; // 하둡분산파일시스템에 저장될 파일경로 및 파일명

        Path path = new Path(hadoopFile); // 하둡분산파일시스템에 저장가능한 객체로 변환

        log.info("HDFS Exists : " + hdfs.exists(path)); // 기존 하둡에 존재하는지 로그 찍기

        if (hdfs.exists(path)) { // 파일이 존재하면
            hdfs.delete(path, true); // 기존에 업로드된 파일 삭제
        }

        Path localPath = new Path(CmmUtil.nvl(pDTO.getLocalUploadPath()) + "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName())); // 업로드할 파일 명

        hdfs.copyFromLocalFile(localPath, path); // 파일 업로드
        hdfs.close();

        log.info(this.getClass().getName() + ".uploadHdfsFile End!");
    }

    @Override
    public void uploadHdfsFileContents(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".uploadHdfsFileContents Start!");

        FileSystem hdfs = null; // 하둡분산파일시스템 초기화

        List<String> contentList = pDTO.getContentList(); // 하둡에 저장할 파일 내용
        log.info("Upload ContentsList Cnt : " + contentList.size()); // 로그로 값 확인

        hdfs = FileSystem.get(this.getHadoopConfiguration()); // 하둡분산파일시스템 연결 및 설정

        String hadoopUploadFilePath = CmmUtil.nvl(pDTO.getHadoopUploadPath()); // 하둡에 파일을 업로드 할 폴더

        String hadoopUploadFileName = CmmUtil.nvl(pDTO.getHadoopUploadFileName()); // 하둡에 업로드 할 파일명

        hdfs.mkdirs(new Path(hadoopUploadFilePath)); // 하둡에 폴더 생성

        String hadoopFile = hadoopUploadFilePath + "/" + hadoopUploadFileName; // 하둡분산파일시스템에 저장될 파일경로 및 파일명

        Path path = new Path(hadoopFile); // 저장가능한 객체로 변환

        log.info("HDFS Exists : " + hdfs.exists(path)); // 존재하는지 로그 찍기

        if(hdfs.exists(path)) { // 존재한다면
            hdfs.delete(path, true); // 기존 업로드 된 파일 삭제
        }

        FSDataOutputStream outputStream = hdfs.create(path);

        contentList.forEach(log -> {
            try {
                outputStream.writeChars(log + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        outputStream.close();

        log.info(this.getClass().getName() + ".uploadHdfsFileContents End!");
    }
}
