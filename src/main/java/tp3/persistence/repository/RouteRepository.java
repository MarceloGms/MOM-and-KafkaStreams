package tp3.persistence.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import tp3.persistence.entity.Route;

@Repository
public interface RouteRepository extends JpaRepository<Route, Long> {
   
}
