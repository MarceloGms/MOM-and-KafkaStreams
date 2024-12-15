package tp3.persistence.repository;

import org.springframework.stereotype.Repository;

import tp3.persistence.entity.Supplier;

import org.springframework.data.jpa.repository.JpaRepository;

@Repository
public interface SupplierRepository extends JpaRepository<Supplier, Long> {
   
}
