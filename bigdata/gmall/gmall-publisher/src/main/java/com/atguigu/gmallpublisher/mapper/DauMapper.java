package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //查询日活总数的抽象方法
    public Integer selectDauTotal(String data);

    //查询分时活跃总数
    public List<Map> selectDauTotalHourMap(String date);
}
