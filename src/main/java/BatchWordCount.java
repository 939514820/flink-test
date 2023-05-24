
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理
 */
public class BatchWordCount  {
    public static void main(String[] args)  throws Exception{
        //1、创建执行的环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2、从文件中读取数据   得到数据源
        DataSource<String> lineDataSource = env.readTextFile("D:\\IdeaProjects\\flink-test\\src\\main\\resources\\input\\words.txt");
        //3、将每行数据进行分词，，转换成二元组的类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            //将一行文本进行分词
            String[] words = line.split(" ");
            for (String word : words) {
                //将每个分词转换成二元组输出返回
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //4、按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        //5、分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        sum.print();

    }

}
