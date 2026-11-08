-- One PostgreSQL server, one database per service (build guide's 8 GB-friendly layout).
-- No service reads another service's database.
CREATE DATABASE skybook_flights;
CREATE DATABASE skybook_booking;
CREATE DATABASE skybook_inventory;
CREATE DATABASE skybook_payment;
CREATE DATABASE skybook_notification;
CREATE DATABASE skybook_test;
