package tp3.api;

import org.springframework.beans.factory.annotation.Autowired;
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
}
