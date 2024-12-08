package entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Route {
   private String origin;
   private String destination;
   private String transportType;
   private String operator;
   private int capacity;
}
