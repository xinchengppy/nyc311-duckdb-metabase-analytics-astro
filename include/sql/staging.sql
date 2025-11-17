-- Staging table for NYC 311 data

-- Create table once (idempotent)
CREATE TABLE IF NOT EXISTS staging.nyc_311 (
    unique_key BIGINT,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    agency TEXT,
    agency_name TEXT,
    complaint_type TEXT,
    descriptor TEXT,
    location_type TEXT,
    incident_zip TEXT,
    incident_address TEXT,
    street_name TEXT,
    cross_street_1 TEXT,
    cross_street_2 TEXT,
    intersection_street_1 TEXT,
    intersection_street_2 TEXT,
    address_type TEXT,
    city TEXT,
    landmark TEXT,
    facility_type TEXT,
    status TEXT,
    due_date TIMESTAMP,
    resolution_description TEXT,
    resolution_action_updated_date TIMESTAMP,
    community_board TEXT,
    bbl TEXT,
    borough TEXT,
    x_coordinate_state_plane DOUBLE,
    y_coordinate_state_plane DOUBLE,
    open_data_channel_type TEXT,
    park_facility_name TEXT,
    park_borough TEXT,
    vehicle_type TEXT,
    taxi_company_borough TEXT,
    taxi_pick_up_location TEXT,
    bridge_highway_name TEXT,
    bridge_highway_direction TEXT,
    road_ramp TEXT,
    bridge_highway_segment TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    location TEXT
);

DELETE FROM staging.nyc_311;

-- Insert the new CSV data
INSERT INTO staging.nyc_311
SELECT
    unique_key,
    TRY_CAST(created_date AS TIMESTAMP),
    TRY_CAST(closed_date AS TIMESTAMP),
    agency,
    agency_name,
    complaint_type,
    descriptor,
    location_type,
    incident_zip,
    incident_address,
    street_name,
    cross_street_1,
    cross_street_2,
    intersection_street_1,
    intersection_street_2,
    address_type,
    city,
    landmark,
    facility_type,
    status,
    TRY_CAST(due_date AS TIMESTAMP),
    resolution_description,
    TRY_CAST(resolution_action_updated_date AS TIMESTAMP),
    community_board,
    bbl,
    borough,
    TRY_CAST(x_coordinate_state_plane AS DOUBLE),
    TRY_CAST(y_coordinate_state_plane AS DOUBLE),
    open_data_channel_type,
    park_facility_name,
    park_borough,
    vehicle_type,
    taxi_company_borough,
    taxi_pick_up_location,
    bridge_highway_name,
    bridge_highway_direction,
    road_ramp,
    bridge_highway_segment,
    TRY_CAST(latitude AS DOUBLE),
    TRY_CAST(longitude AS DOUBLE),
    location
FROM read_csv('{{CSV_FILE}}', nullstr='N/A')
WHERE
    unique_key IS NOT NULL
    AND created_date IS NOT NULL
    AND complaint_type IS NOT NULL
    AND borough IS NOT NULL
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND incident_zip IS NOT NULL;