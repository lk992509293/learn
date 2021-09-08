package com.atguigu.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class JoinReduce extends Reducer<Text, JoinBean, JoinBean, NullWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<JoinBean> joinArr = new ArrayList<>();
        JoinBean tmpPd = new JoinBean();

        //提取输入集合values
        for (JoinBean value : values) {
            if ("order".equals(value.getFlag())) {
                //每一次遍历都临时创建一个JoinBean对象
                JoinBean tmpBean = new JoinBean();

                //将value中的数据都拷贝到tmpBean中
                try {
                    BeanUtils.copyProperties(tmpBean,value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }

                //将临时JoinBean对象存放到集合中
                joinArr.add(tmpBean);
            } else {
                try {
                    BeanUtils.copyProperties(tmpPd, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        //遍历集合orderBeans,替换掉每个orderBean的pid为pname,然后写出
        for (JoinBean joinBean : joinArr) {
            joinBean.setPname(tmpPd.getPname());

            //输出
            context.write(joinBean, NullWritable.get());
        }
    }
}
