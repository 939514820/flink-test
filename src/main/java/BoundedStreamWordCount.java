package com.lx.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理
 * 有界的
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
//        1、创建流式的执行环境（与批处理区别在于：环境的创建不同）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        2、读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("D:\\IdeaProjects\\flink-test\\src\\main\\resources\\input\\words.txt");
//        3、进行转换
        SingleOutputStreamOperator<Tuple2<String,Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String,Long>> out) -> {
            //将一行文本进行分词
            String[] words = line.split(" ");
            for (String word : words) {
                //将每个分词转换成二元组输出返回
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
//        4、分组操作
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);
//         5、求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);
//        6、打印输出
        sum.print();
//    每来一个数据，就执行的操作
//        7、启动执行
        env.execute();
//        下面结果前面的数字代表个人电脑内核的大小，假如是八核的，那么前面的数字就会是1-8之间的数字
    }
}
