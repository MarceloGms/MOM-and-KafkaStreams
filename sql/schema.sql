-- Drop Supplier table
DROP TABLE IF EXISTS Supplier;

-- Drop Route table
DROP TABLE IF EXISTS Route;

-- Create Supplier table
CREATE TABLE IF NOT EXISTS Supplier (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL CHECK (type IN ('Bus', 'Taxi', 'Train', 'Metro', 'Scooter')) NOT NULL
);

-- Create Route table
CREATE TABLE IF NOT EXISTS Route (
    routeId SERIAL PRIMARY KEY,
    origin VARCHAR(255) NOT NULL,
    destination VARCHAR(255) NOT NULL,
    type VARCHAR(255) CHECK (type IN ('Bus', 'Taxi', 'Train', 'Metro', 'Scooter')) NOT NULL,
    operator VARCHAR(255) CHECK (operator IN ('OperatorA', 'OperatorB', 'OperatorC')) NOT NULL,
    capacity INT NOT NULL
);