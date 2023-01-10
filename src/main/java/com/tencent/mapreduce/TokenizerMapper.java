package com.tencent.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * mapper
 */
class TokenizerMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Text keyOut;
        // 定义整数1,每个单词计数一次
        IntWritable valueOut = new IntWritable(1);
        /*
         * 构造一个用来解析输入value值的StringTokenizer对象
         * Java默认的分隔符是"空格""制表符('\t')""换行符('\n')""回车符* ('\r')"
         */
        StringTokenizer token = new StringTokenizer(value.toString());
        while (token.hasMoreTokens()) {
            // 返回从当前位置到下一个分隔符的字符串
            keyOut = new Text(token.nextToken());
            // map方法输出键值对:输出每个被拆分出来的单词,以及计数1
            context.write(keyOut, valueOut);
        }
    }
}
