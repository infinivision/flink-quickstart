package org.infinivision.flink.examples.io;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class S3FileSystemDemo {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

//        DataSet<String> input = env.fromElements("Hello", "World");
        DataSet<String> input = env.readTextFile("s3://flink/testfile.txt");
//        input.print();
        input.writeAsText("s3://flink/output.txt", WriteMode.OVERWRITE);
        env.execute("Flink Ceph S3 Demo");
    }
}
