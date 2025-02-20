-- init.sql
-- Create a custom database and switch to it, then create a table with sample data.

CREATE DATABASE main_db;

\connect main_db;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (name)
VALUES ('Alice'), ('Bob'), ('Charlie');
