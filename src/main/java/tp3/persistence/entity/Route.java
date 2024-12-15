package tp3.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Route {
   private String routeId;
   private String origin;
   private String destination;
   private String transportType;
   private String operator;
   private int capacity;
}
