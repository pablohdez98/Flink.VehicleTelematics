package master;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Objects;

public class VehicleTelematics {
    static String outFolderPath;
    public static void main(String[] args) {

        if (args.length != 2) {
            throw new IllegalArgumentException("Input file and output folder should be given");
        }
        String inFilePath = args[0];
        outFolderPath = args[1];

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<String> source = env.readTextFile(inFilePath);

        // Map the data in tuples of 8, and assign timestamps and watermarks
        SingleOutputStreamOperator<Event> events = source
                .map((MapFunction<String, Event>) Event::new)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((element, timeStamp) -> element.event.f0 * 1000));

        speedRadar(events);
        // Execution
        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Detects cars that overcome the speed limit of 90 mph
     * @param events input data
     */
    private static void speedRadar(SingleOutputStreamOperator<Event> events) {
        events
                .filter((FilterFunction<Event>) in -> in.event.f2 > 90)
                .map(new MapFunction<Event, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Event in) {
                        return new Tuple6<>(in.event.f0, in.event.f1, in.event.f3, in.event.f6, in.event.f5, in.event.f2);
                    }
                })
                .writeAsCsv(outFolderPath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
}