CREATE TABLE DBInfo (
    id SERIAL PRIMARY KEY,
    route_supplier TEXT NOT NULL,
    route_capacity INTEGER NOT NULL
);

-- Table for the Routes topic
CREATE TABLE Routes (
    id SERIAL PRIMARY KEY,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    passenger_capacity INTEGER NOT NULL,
    transport_type TEXT CHECK (transport_type IN ('Bus', 'Taxi', 'Train', 'Metro', 'Scooter')) NOT NULL,
    operator_name TEXT NOT NULL
);

-- Table for the Trips topic
CREATE TABLE Trips (
    id SERIAL PRIMARY KEY,
    route_id INTEGER REFERENCES Routes(id),
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    passenger_name TEXT NOT NULL,
    transport_type TEXT CHECK (transport_type IN ('Bus', 'Taxi', 'Train', 'Metro', 'Scooter')) NOT NULL
);
