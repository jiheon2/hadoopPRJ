package com.example.hadoopprj.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
@Getter
@Setter
public class HadoopDTO {
    private String hadoopUploadFileName; // 하둡파일명
    private String hadoopUploadPath; // 하둡 경로
    private String localUploadFileName; // 로컬 파일명
    private String localUploadPath; // 로컬 파일 주소
    long lineCnt; // GZ 업로드 할 라인 수
    List<String> contentList; // GZ 파일내용
    String regExp; // 정규표현식
}
