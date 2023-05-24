package com.lx.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 真正的流处理
 * 无界处理
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
//     1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      2、创建文本流   在服务器上部署navicat ，进行连接。在这里我就不展示了，只把核心代码展示一下，当然在这里只是把他写死了，平常在项目文件里面通过配置文件进行读取或者设置参数的形式进行
//        DataStreamSource<String> lineDataStram = env.socketTextStream("Hadoop02", 222);
//        从参数中提取主机号和端口号  需要配置program  arguments:--host hadoop012  --port 222
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostName = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");
        DataStreamSource<String> lineDataStram = env.socketTextStream(hostName, port);

//        3、进行转换
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStram.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            //将一行文本进行分词
            String[] words = line.split(" ");
            for (String word : words) {
                //将每个分词转换成二元组输出返回
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
//        4、分组操作
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);
//         5、求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);
//        6、打印输出
        sum.print();
//    每来一个数据，就执行的操作
//        7、启动执行
        env.execute();
//        启动成功之后，在服务里面进行输入 比如hello world，这边控制就会解析出你输入的信息，跟有界流输出的结果是一个样子

    }
}
