package tp3.persistence.entity;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Results {
   private Map<String, Object> schema;
   private Map<String, Object> payload;
}
