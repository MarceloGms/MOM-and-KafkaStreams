package tp3.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import tp3.persistence.entity.Route;
import tp3.persistence.repository.RouteRepository;

@Service
public class RouteService {

   @Autowired
   private RouteRepository routeRepository;

   public Route createRoute(Route route) {
      return routeRepository.save(route);
   }

   public List<Route> getRoutes() {
      return routeRepository.findAll();
   }

   public Route getRouteById(Long id) {
      return routeRepository.findById(id).orElse(null);
   }
}
