SELECT
    ride_request_time,
    ride_cancel_time,
    avg_waiting_time_cancellation,
    request_ts,
    cancel_ts,
    (
        SELECT AVG(pickup_ts - request_ts)
        FROM ride_requests
        WHERE pickup_ts IS NOT NULL
    ) AS average_wait_time_before_pickup
FROM
    (
        SELECT
            r.request_ts AS ride_request_time,
            r.cancel_ts AS ride_cancel_time,
            AVG (r.cancel_ts - r.request_ts) AS avg_waiting_time_cancellation,
            r.request_ts,
            r.cancel_ts,
            r.pickup_ts
        FROM
            ride_requests r
        WHERE
            r.cancel_ts IS NOT NULL
      
      GROUP BY r.request_ts, r.cancel_ts, r.pickup_ts
      ORDER BY avg_waiting_time_cancellation DESC
    ) subquery;
