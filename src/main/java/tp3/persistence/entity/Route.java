package tp3.persistence.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
public class Route {
   @Id
   @GeneratedValue(strategy = GenerationType.IDENTITY)
   private long routeId;
   private String origin;
   private String destination;
   private String type;
   private String operator;
   private int capacity;

   public String getType() {
      return type;
  }
}
