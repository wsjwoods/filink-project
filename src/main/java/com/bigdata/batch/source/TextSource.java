package com.bigdata.batch.source;

import org.apache.flink.api.java.ExecutionEnvironment;

public class TextSource {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //final ObjectMapper mapper = new ObjectMapper();
        //DataSet<String> stringDataSource = env.readTextFile("D:\\workspace\\filink-project\\src\\main\\resources\\People.json");

        //解析json的时候注意
        // ObjectMapper来解析
        //如果你ObjectMapper写在map里面，会存在，没处理一条数据就创建一个ObjectMapper对象，是不行
//        stringDataSource.map(new MapFunction<String, People>() {
//            public People map(String s) throws Exception {
//                People People = mapper.readValue(s, People.class);
//                return People;
//            }
//        }).print();

        //解析csv格式的数据

//        env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv")
//                .includeFields("101")
//                .types(String.class, String.class).print();
        env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv")
                .pojoType(People.class, "name", "age", "phone").print();


        //mysql作为数据源
//        env.createInput(
//                JDBCInputFormat.buildJDBCInputFormat()
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setDBUrl("jdbc:mysql://192.168.148.12:3306/test")
//                .setQuery("select * from People")
//                     //   .setFetchSize(2)
//                .setUsername("root")
//                .setPassword("123456")
//                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
//                        BasicTypeInfo.STRING_TYPE_INFO))
//                .finish()
//        ).print();



    }
}


