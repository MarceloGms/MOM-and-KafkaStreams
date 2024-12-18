package tp3.api;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import tp3.service.ResultsService;

@RestController
public class ResultsController {

   @Autowired
   private ResultsService resultsService;

   @GetMapping("/4")
   public List<Map<String, Object>> getResults4() {
      return resultsService.getResults4();
   }

   @GetMapping("/5")
   public List<Map<String, Object>> getResults5() {
      return resultsService.getResults5();
   }

   @GetMapping("/6")
   public List<Map<String, Object>> getResults6() {
      return resultsService.getResults6();
   }

   @GetMapping("/7")
   public List<Map<String, Object>> getResults7() {
      return resultsService.getResults7();
   }

   @GetMapping("/8")
   public List<Map<String, Object>> getResults8() {
      return resultsService.getResults8();
   }

   @GetMapping("/9")
   public List<Map<String, Object>> getResults9() {
      return resultsService.getResults9();
   }

   @GetMapping("/10")
   public List<Map<String, Object>> getResults10() {
      return resultsService.getResults10();
   }

   @GetMapping("/11")
   public List<Map<String, Object>> getResults11() {
      return resultsService.getResults11();
   }

   @GetMapping("/12")
   public List<Map<String, Object>> getResults12() {
      return resultsService.getResults12();
   }

   @GetMapping("/13")
   public List<Map<String, Object>> getResults13() {
      return resultsService.getResults13();
   }

   @GetMapping("/14")
   public List<Map<String, Object>> getResults14() {
      return resultsService.getResults14();
   }

   @GetMapping("/15")
   public List<Map<String, Object>> getResults15() {
      return resultsService.getResults15();
   }

   @GetMapping("/16")
   public List<Map<String, Object>> getResults16() {
      return resultsService.getResults16();
   }
   
}
