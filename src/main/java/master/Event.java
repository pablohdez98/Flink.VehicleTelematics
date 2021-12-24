package master;

import org.apache.flink.api.java.tuple.Tuple8;

public class Event {
    Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event = new Tuple8<>();

    public Event(String eventLine) {
        String[] fieldArray = eventLine.split(",");
        this.event.f0 = Integer.parseInt(fieldArray[0]);
        this.event.f1 = Integer.parseInt(fieldArray[1]);
        this.event.f2 = Integer.parseInt(fieldArray[2]);
        this.event.f3 = Integer.parseInt(fieldArray[3]);
        this.event.f4 = Integer.parseInt(fieldArray[4]);
        this.event.f5 = Integer.parseInt(fieldArray[5]);
        this.event.f6 = Integer.parseInt(fieldArray[6]);
        this.event.f7 = Integer.parseInt(fieldArray[7]);
    }
}