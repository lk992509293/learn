package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {
    public static List<String> getAnalyze(String words) {
        //缓存分词后的数据结果
        ArrayList<String> list = new ArrayList<>();

        StringReader reader = new StringReader(words);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String lexemeText = next.getLexemeText();
                list.add(lexemeText);
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    //测试
    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeyWordUtil.getAnalyze(text));
    }
}
