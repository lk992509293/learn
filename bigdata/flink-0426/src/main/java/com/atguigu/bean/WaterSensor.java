package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 水位传感器：用于接收水位数据
 *
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */
@Data //表示添加get和set方法
@NoArgsConstructor //表示添加无参构造器
@AllArgsConstructor //表示添加满参构造器
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}

