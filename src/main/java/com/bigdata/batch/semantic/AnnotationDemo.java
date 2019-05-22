package com.bigdata.batch.semantic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class AnnotationDemo {

    public static void main(String[] args) throws Exception {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, String, String>> types = env.readCsvFile("D:\\workspace\\filink-project\\src\\main\\resources\\test.csv")
                .types(String.class, String.class, String.class);

        //语义标注不能用于匿名内部类

        types.map(new myMapFunctionRead()).print();
    }

    @FunctionAnnotation.ForwardedFields("f0->f3")
    //这个标注的意思是，第四个元素其实就是第一个元素copy过去的
    //如果我们标注出错了，代码是否还能识别
    //经过验证，ForwardedFields语义标注错了，也不会出错
    public static class myMapFunctionForwarded implements MapFunction<Tuple3<String,String,String>,Tuple4<String,String,String,String>>{

        @Override
        public Tuple4<String, String, String, String> map(Tuple3<String, String, String> t) throws Exception {
            return new Tuple4<>(t.f0,t.f1,t.f2,t.f0);
        }
    }

    @FunctionAnnotation.NonForwardedFields("f2")
    //与Forwarded相反，它是标注哪些字段不是copy的
    //在NonForwarded中，输入输出的参数类型保持一致，不然会报错
    //在NonForwarded中,标注的内容要正确，否则，会报错
    public static class myMapFunctionNonForwarded implements MapFunction<Tuple3<String,String,String>,Tuple3<String,String,String>>{
        @Override
        public Tuple3<String, String, String> map(Tuple3<String, String, String> t) throws Exception {
            return new Tuple3<>(t.f0,t.f1,t.f0+"-"+t.f1);
        }
    }

    @FunctionAnnotation.ReadFields("f0;f1")
    //从字面意思就可以知道，它表明，哪些字段是被读到的
    //从注解知道，哪些字段是要被加载的，从而提升我们的程序性能
    //跟sql的谓词下推有点类似
    //就算注解错了，也不会报错
    public static class myMapFunctionRead implements MapFunction<Tuple3<String,String,String>,Tuple3<String,String,String>>{
        @Override
        public Tuple3<String, String, String> map(Tuple3<String, String, String> t) throws Exception {
            return new Tuple3<>(t.f0,t.f1,t.f0+"-"+t.f1);
        }
    }


}
