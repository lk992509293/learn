package com.atguigu.gmallpublisher.collector;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Collector {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String selectDauTotal(@RequestParam("date") String date) {
        //1.获取service层的数据
        Integer dauTotal = publisherService.getDauTotal(date);
        Double amountTotal = publisherService.getOrderAmountTotal(date);

        //2.创建map集合用来存放新增日活数据
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //3.创建map集合用来存放新增设备
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 39);

        //创建存放交易总额的map集合
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name","新增交易额");
        gmvMap.put("value",amountTotal);

        //4.创建一个list集合，用来存放结果数据
        ArrayList<Map> list = new ArrayList<>();
        list.add(dauMap);
        list.add(devMap);
        list.add(gmvMap);

        //5.将list转换为json字符串
        return JSONObject.toJSONString(list);
    }

    /**
     * @Author:lk
     * @Description:封装分时数据
     * @DateTime:2021/8/31 20:54
     * @Params:* @param null
     * @Return:
     */
    @RequestMapping("realtime-hours")
    public String selectTotalHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        //1.创建map来存放结果数据
        HashMap<String, Map> res = new HashMap<>();

        //2.获取serivce中的数据
        //2.1获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            //2.2获取今天的分时数据
            todayMap = publisherService.getDauHourTotal(date);

            //2.3获取昨天的分时数据
            yesterdayMap = publisherService.getDauHourTotal(yesterday);
        } else if ("order_amount".equals(id)) {
            //获取今天的交易总额
            todayMap = publisherService.getOrderAmountHourMap(date);

            //获取昨天的交易总额
            yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
        }

        //3.将获取到的数据封装到结果map集合中
        res.put("yesterday", yesterdayMap);
        res.put("today", todayMap);

        return JSONObject.toJSONString(res);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") Integer startpage,
                                @RequestParam("size") Integer size,
                                @RequestParam("keyword") String keyword) throws IOException {
        //获取Service层封装好的数据
        Map map = publisherService.SaleDetail(date, startpage, size, keyword);
        return JSONObject.toJSONString(map);
    }
}
