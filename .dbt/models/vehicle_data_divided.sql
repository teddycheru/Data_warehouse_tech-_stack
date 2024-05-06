-- vehicle_data_divided.sql

SELECT
    track_id,
    type,
    traveled_d,
    avg_speed,
    lat,
    lon,
    speed,
    lon_acc,
    lat_acc,
    time
FROM
    {{ source('vehicle_data', 'vehicle_data') }}