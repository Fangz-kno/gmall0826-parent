package com.atguigu.gmall0826.gmall0826logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0826.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
@Slf4j
@RestController
public class LoggerControler {
        @Autowired
        KafkaTemplate kafkaTemplate;
    @PostMapping("log")
public  String doLog(@RequestParam("logString") String logString){

//        System.out.println(logString);
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
//        TODO
        String logJsonString = jsonObject.toJSONString();
        System.out.println(logJsonString);
        log.info(logJsonString);
            //  发送kafka
            if("startup".equals( jsonObject.getString("type"))){
                    kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,logJsonString);
            }else{
                    kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,logJsonString);
            }
        return "success";

}
}
