package com.example.hadoopprj;

import com.example.hadoopprj.component.IHdfsExam;
import com.example.hadoopprj.component.impl.Exam01;
import com.example.hadoopprj.component.impl.Exam02;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
@Log4j
@RequiredArgsConstructor
@SpringBootApplication
public class HadoopPrjApplication implements CommandLineRunner {

    private Exam01 exam01;
    private Exam02 exam02;

    @Override
    public void run(String... args) throws Exception {

        log.info("안녕하세요~~ 하둡 프로그래밍 실습!");

        log.info("첫번째 실습");
        exam01.doExam();

        log.info("두번째 실습");
        exam02.doExam();

        log.info("안녕하세요~~ 하둡 프로그래밍 실습 끝!");
    }

    public static void main(String[] args) {
        SpringApplication.run(HadoopPrjApplication.class, args);
    }

}
