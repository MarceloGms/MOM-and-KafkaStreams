package tp3.api;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import tp3.persistence.entity.Route;
import tp3.service.RouteService;

@RestController
@RequestMapping("/route")
public class RouteController {

   @Autowired
   private RouteService routeService;
   
   @PostMapping
   public Route createRoute(@RequestBody Route route) {
      return routeService.createRoute(route);
   }

   @GetMapping
   public List<Route> getRoutes() {
      return routeService.getRoutes();
   }

   @GetMapping("/{id}")
   public Route getRouteById(@PathVariable("id") Long id) {
      return routeService.getRouteById(id);
   }
}
