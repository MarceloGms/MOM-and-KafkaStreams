package tp3.service;

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
   
}
