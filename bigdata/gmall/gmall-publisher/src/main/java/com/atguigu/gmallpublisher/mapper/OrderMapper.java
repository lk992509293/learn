package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    //1.查询当日交易总额
    public Double selectOrderAmountTotal(String date);

    //2.查询当日分时的交易总额
    public List<Map> selectOrderAmountHourMap(String date);
}
