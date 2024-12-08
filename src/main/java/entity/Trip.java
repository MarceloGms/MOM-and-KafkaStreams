package entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Trip {
   private String routeId;
   private String passengerName;
   private String origin;
   private String destination;
   private String transportType;
}
