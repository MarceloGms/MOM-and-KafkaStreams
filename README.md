# Urban Mobility Data Processing with Apache Kafka  

This repository contains the implementation for **Project #3** of the Enterprise Application Integration course at the University of Coimbra. The project leverages Apache Kafka and Kafka Streams to develop a fault-tolerant, scalable, and asynchronous system for processing urban mobility data.  

## Project Overview  

The project simulates an urban mobility company where various Kafka topics manage data such as:  

- **DBInfo**: Contains route suppliers and route passenger capacities.  
- **Routes**: Simulates route data, including capacities, origins, destinations, and transport types.  
- **Trips**: Contains customer trip data linked to routes.  
- **Results**: Stores aggregated metrics for further processing or visualization.  

The system includes multiple stand-alone applications to produce, process, and consume data streams. Kafka Streams is used for real-time data aggregation and computation.  

## Objectives  

1. Learn to create asynchronous and message-oriented applications.  
2. Use Kafka Streams to perform real-time data processing.  
3. Implement fault-tolerant mechanisms with Kafka's multi-broker setup.  
4. Serialize and deserialize data using formats like JSON or AVRO.  

## Features  

The system supports the following functionalities:  

1. Add and list route suppliers.  
2. Add and modify routes.  
3. Compute:  
   - Passengers per route  
   - Available seats per route  
   - Occupancy percentages  
   - Total passengers and available seating  
   - Average passengers per transport type  
   - Transport types with highest/lowest usage  
   - Operators with the most occupancy  
   - Passengers with the most trips  
4. Aggregate metrics using Kafka Streams (e.g., tumbling time windows for hourly analysis).  
