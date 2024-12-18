package tp3.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ResultsService {

   @Autowired
   private JdbcTemplate jdbcTemplate;

   public List<Map<String, Object>> getResults4() {
      String sql = "SELECT * FROM \"ResultsPassengersPerRoute\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults5() {
      String sql = "SELECT * FROM \"ResultsAvailableSeatsPerRoute\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults6() {
      String sql = "SELECT * FROM \"ResultsOccupancyPercentagePerRoute\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults7() {
      String sql = "SELECT * FROM \"ResultsTotalPassengerCount\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults8() {
      String sql = "SELECT * FROM \"ResultsTotalSeatingAvailable\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults9() {
      String sql = "SELECT * FROM \"ResultsTotalOccupancyPercentage\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults10() {
      String sql = "SELECT * FROM \"ResultsAveragePassengersPerTransportType\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults11() {
      String sql = "SELECT * FROM \"ResultsHighestTransportType\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults12() {
      String sql = "SELECT * FROM \"Results12\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults13() {
      String sql = "SELECT * FROM \"Results13\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults14() {
      String sql = "SELECT * FROM \"Results14\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults15() {
      String sql = "SELECT * FROM \"Results15\"";
      return jdbcTemplate.queryForList(sql);
   }

   public List<Map<String, Object>> getResults16() {
      String sql = "SELECT * FROM \"Results16\"";
      return jdbcTemplate.queryForList(sql);
   }
}
