package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {
    //获取日活跃用户数据
    public Integer getDauTotal(String date);

    //获取用户当日分时活跃用户数据
    public Map getDauHourTotal(String date);

    //交易额总数
    public Double getOrderAmountTotal(String date);

    //获取用户当日分时交易总额
    public Map<String, Double> getOrderAmountHourMap(String date);

    //获取出售订单明细数据
    public Map SaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException;
}
