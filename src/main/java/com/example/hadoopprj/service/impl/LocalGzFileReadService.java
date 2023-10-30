package com.example.hadoopprj.service.impl;

import com.example.hadoopprj.dto.HadoopDTO;
import com.example.hadoopprj.service.ILocalGzFileReadService;
import com.example.hadoopprj.util.CmmUtil;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

@Log4j
@Service
public class LocalGzFileReadService implements ILocalGzFileReadService {
    @Override
    public List<String> readLocalGzFileCnt(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".readLocalGzFile Start!");

        long readCnt = pDTO.getLineCnt(); // 읽을 라인 수
        log.info("readCnt : " + readCnt);

        List<String> logList = new ArrayList<>(); // 로그 정보를 저장할 리스트 객체

        String localFile = CmmUtil.nvl(pDTO.getLocalUploadPath()) + "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName()); // 읽을 파일

        log.info("localFile : " + localFile);

        File localGzFile = new File(localFile); // Gz 파일 가져오기

        FileInputStream fis = new FileInputStream(localGzFile); // 파일처리를 위해 파일을 스트림 형태로 변환

        GZIPInputStream gzis = new GZIPInputStream(fis); // Gz파일로 압축된 파일스트림을 압축해제한 스트림으로 변환

        InputStreamReader streamReader = new InputStreamReader(gzis); // Gz스트림 내용을 읽기

        try (BufferedReader lineReader = new BufferedReader(streamReader)) { // 스트림으로 내용을 읽는 경우는 무조건 BufferedReader을 통해 읽자
            long idx = 0; // 읽은 라인 횟수

            String line; // 읽은 라인의 값이 저장되는 변수

            while ((line = lineReader.readLine()) != null) {
                log.info("line : " + line);
                logList.add(line);

                idx++; // 읽은 라인 수

                if (readCnt == idx) { // 다 읽으면 브레이크
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Gzip 파일 읽기 실패했습니다.", e);
        }

        gzis.close();
        gzis = null;

        fis.close();
        fis = null;

        streamReader.close();
        streamReader = null;

        log.info(this.getClass().getName() + ".readLocalGzFile End!");

        return logList;
    }

    @Override
    public List<String> readLocalGzFileIP(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".readLocalGzFileIP Start!");

        String localFile = CmmUtil.nvl(pDTO.getLocalUploadPath()) + "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName()); // 하둡에 올릴 파일경로 및 파일 명

        log.info("localFile : " + localFile);

        List<String> logList = new ArrayList<>(); // 로그 정보 저장할 객체

        File localGzFile = new File(localFile); // 파일 가져오기

        FileInputStream fis = new FileInputStream(localGzFile); // 파일을 스트림 형태로 변환

        GZIPInputStream gzis = new GZIPInputStream(fis); //GZ파일을 압축해제한 스트림으로 변경

        InputStreamReader streamReader = new InputStreamReader(gzis); // Gz스트림 내용 읽기

        try (BufferedReader lineReader = new BufferedReader(streamReader)) {

            String line; // 읽는 라인 값이 저장되는 변수

            String exp = CmmUtil.nvl(pDTO.getRegExp()); // 정규식 표현식
            log.info("exp : " + exp);

            while ((line = lineReader.readLine()) != null) {
                String ip = line.substring(0, line.indexOf(" ")); // IP값만 가져오기

                /*
                정규 표현식에 맞으면 라인 가져오기
                10.56.xxx.xxx IP 대역 가져오기
                자바 정규식은 CentOS의 grep 명령어와 달리 비교대상과 정규식 패턴이 일치해야만 탐지 가능
                로그에서 IP 추출하고, 그 IP에 대한 정규식 패턴이 맞는지 체크함
                 */

                if (Pattern.matches(exp, ip)) {
                    logList.add(line); // 패턴이 맞는 로그 라인 전체 List에 저장
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Gzip 파일 읽기 실패했습니다.", e);
        }

        gzis.close();
        gzis = null;

        fis.close();
        fis = null;

        streamReader.close();
        streamReader = null;

        log.info(this.getClass().getName() + ".readLocalGzFile End!");

        return logList;
    }
}
