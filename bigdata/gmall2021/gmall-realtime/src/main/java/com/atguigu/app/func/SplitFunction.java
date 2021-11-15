package com.atguigu.app.func;

import com.atguigu.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String keyWord) {
        //使用分词器工具对词条进行拆分
        List<String> words = KeyWordUtil.getAnalyze(keyWord);
        for (String word : words) {
            collect(Row.of(word));
        }
    }
}
