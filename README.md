# Flink. Vehicle Telematics

Drivers, fleet owners, transport operations, insurance companies are stakeholders of vehicle
monitoring applications which need to have analytical reporting on the mobility patterns of their vehicles, as well as real-time views in order to support quick and efficient decisions towards eco-friendly moves, cost-effective maintenance of vehicles, improved navigation, safety and adaptive risk management. Vehicle sensors do continuously provide data, while on-the-move, they are processed in order to provide valuable information to stakeholders. Applications identify speed violations, abnormal driver behaviors, and/or other extraordinary vehicle conditions, produce statistics per driver/vehicle/fleet/trip, correlate events with map positions and route, assist navigation, monitor fuel consumptions, and perform many other reporting and alerting functions.

In this project we consider that each vehicle reports a position event every 30 seconds with the following format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos.

- Being Time a timestamp (integer) in seconds identifying the time at which the position event was emitted
- VID is an integer that identifies the vehicle
- Spd (0 - 100) is an integer that represents the speed in mph (miles per hour)
- XWay (0 . . .L−1) identifies the highway from which the position report is emitted
- Lane (0 . . . 4) identifies the lane of the highway from which the position report is emitted (0 if it is an entrance ramp (ENTRY), 1 − 3 if it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT)).
- Dir (0 . . . 1) indicates the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling.
- Seg (0 . . . 99) identifies the segment from which the position report is emitted
- Pos (0 . . . 527999) identifies the horizontal position of the vehicle as the number of meters from the westernmost point on the highway (i.e., Pos = x)

### Functionalities implemented

The goal of this project is to develop a Java program using Flink implementing the following
functionality:

- SpeedRadar: detects cars that overcome the speed limit of 90 mph.
- AverageSpeedControl: detects cars with an average speed higher than 60 mph between
segments 52 and 56 (both included) in both directions. If a car sends several reports on segments 52 or 56, the ones taken for the average speed are the ones that cover a longer distance.
- AccidentReporter: detects stopped vehicles on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position.

### Requirements

The application has been developed using the versions of the software: Oracle Java 11, Maven, Flink 1.14.0. The project was configured with the flink-quickstart-java maven artifact

### How to Run the Project

The csv file used can be found it at https://dl.lsdupm.ovh/CLOUD/2122/sample-traffic-3xways.csv

To run the project, first we create the jar file:
```
mvn clean package -Pbuild-jar
```
Then, we execute it by using Flink:
```
$FLINK_HOME/bin/flink run -p 3 -c master.VehicleTelematics target/$JAR_FILE $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER 
```

