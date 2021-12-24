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
        averageSpeedControl(events);
        accidentReporter(events);

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

    /**
     * Detects cars with an average speed higher than 60 mph between
     * segments 52 and 56 (both included) in both directions. If a car sends several reports on segments
     * 52 or 56, the ones taken for the average speed are the ones that cover a longer distance
     * @param events input data
     */
    private static void averageSpeedControl(SingleOutputStreamOperator<Event> events) {
        events
            .filter((FilterFunction<Event>) in -> (in.event.f6 >= 52 && in.event.f6 <=56))
            .keyBy(new KeySelector<Event, Tuple3<Integer, Integer, Integer>>() {
                @Override
                public Tuple3<Integer, Integer, Integer> getKey(Event in) {
                    return new Tuple3<>(in.event.f1, in.event.f3, in.event.f5);
                }
            })
            .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
            .apply(new WindowFunction<Event, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>,
                    Tuple3<Integer, Integer, Integer>, TimeWindow>() {
                @Override
                public void apply(Tuple3<Integer, Integer, Integer> key, TimeWindow window, Iterable<Event> input,
                                  Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> collector) {
                    Iterator<Event> iterator = input.iterator();
                    Event first = iterator.next();
                    Event last = first;
                    while(iterator.hasNext()) {
                        last = iterator.next();
                    }
                    if ((first.event.f6 == 52 && last.event.f6 == 56) || (first.event.f6 == 56 && last.event.f6 == 52)) {
                        float distance = Math.abs(last.event.f7 - first.event.f7);
                        float time = Math.abs(last.event.f0 - first.event.f0);
                        double avgSpeed = (distance / time) * 2.23694;

                        if (avgSpeed > 60) {
                            collector.collect(new Tuple6<>(first.event.f0, last.event.f0, first.event.f1, first.event.f3,
                                    first.event.f5, avgSpeed));
                        }
                    }
                }
            })
            .writeAsCsv(outFolderPath + "avespeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    /**
     * Detects stopped vehicles on any segment. A vehicle is stopped when it reports
     * at least 4 consecutive events from the same position.
     * @param events input data
     */
    private static void accidentReporter(SingleOutputStreamOperator<Event> events) {
        events
            .filter((FilterFunction<Event>) in -> (in.event.f2 == 0))
            .keyBy(new KeySelector<Event, Tuple1<Integer>>() {
                @Override
                public Tuple1<Integer> getKey(Event in) {
                    return new Tuple1<>(in.event.f1);
                }
            })
            .countWindow(4, 1)
            .apply(new WindowFunction<Event, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                    Tuple1<Integer>, GlobalWindow>() {
                @Override
                public void apply(Tuple1<Integer> key, GlobalWindow window, Iterable<Event> input,
                                  Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> collector) {
                    Iterator<Event> iterator = input.iterator();
                    Event first = iterator.next();
                    Event last = first;
                    int count = 1;
                    while(iterator.hasNext()) {
                        last = iterator.next();
                        count++;
                    }
                    if (count == 4 && Objects.equals(first.event.f7, last.event.f7)) {
                        collector.collect(new Tuple7<>(first.event.f0, last.event.f0, first.event.f1,
                                first.event.f3, first.event.f6, first.event.f5, first.event.f7));
                    }
                }
            })
            .writeAsCsv(outFolderPath + "accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
}