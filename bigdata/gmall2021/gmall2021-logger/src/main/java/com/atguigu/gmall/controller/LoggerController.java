package com.atguigu.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//添加controller和返回普通对象而不是页面注解
//@RestController  = @Controller+@ResponseBody
@RestController
//使用日志直接落盘的注解
@Slf4j
public class LoggerController {
/*    @RequestMapping("test1")
    public String test1() {
        System.out.println("test1");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name")String name, @RequestParam(value = "age", defaultValue = "18")int age) {
        System.out.println("name:" + age);
        return name + " " + age;
    }*/

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    //将日志落盘并发送到kafka
    @RequestMapping("applog")
    public String getLogger(@RequestParam("param")String jsonStr) {
        //1.将日志打印出来
        System.out.println(jsonStr);

        //2.将日志落盘
        log.info(jsonStr);

        //3.将日志发送到kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";

    }
}
