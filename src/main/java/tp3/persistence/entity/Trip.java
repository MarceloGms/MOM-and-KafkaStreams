package tp3.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Trip {
   private long routeId;
   private String passengerName;
   private String origin;
   private String destination;
   private String transportType;
}
