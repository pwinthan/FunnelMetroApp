 --Steps in funnel analysis. App Download, Signup, Request Ride, Driver Acceptance, Ride, Payment, Review
--Remember that we are looking at these metrics from a user-level; the number of [distinct] users that signed up, 
--requested a ride and completed. A user can reques multiple rides but will only be counted once.
/*The objective of this funnel analysis is to understand the user journey through Metrocar's 
platform and identify areas for improvement and optimization.*/
-- Step 1: User app download
with
  download as (
    select distinct
      (app_download_key)
    from
      app_downloads
  ),
  --Step 2:Signups 
  signups as (
    SELECT DISTINCT
      (user_id)
    FROM
      signups
  ),
  --Step 3:Request Ride (from the signup above)
  request_ride_cte as (
    SELECT DISTINCT
      ride_requests.user_id,
      COUNT(ride_requests.ride_id) AS ride_count
    FROM
      ride_requests
      JOIN signups ON ride_requests.user_id = signups.user_id
    group by ride_requests.user_id
  ),
  --Step 4:DriverAcceptance 
  driveracceptance as (
    select DISTINCT
      ride_requests.user_id,
      COUNT(ride_requests.ride_id) AS ride_count
    FROM
      ride_requests
      JOIN request_ride_cte rr ON ride_requests.user_id = rr.user_id
    WHERE
      ride_requests.accept_ts IS NOT NULL
    GROUP BY ride_requests.user_id
  ),
  --Step 5:Ride 
  ride as (
    select DISTINCT
      ride_requests.user_id,
    COUNT(ride_requests.ride_id) AS ride_count
    FROM
      ride_requests
      JOIN driveracceptance da ON ride_requests.user_id = da.user_id
    WHERE
      ride_requests.dropoff_ts IS NOT NULL
    GROUP BY ride_requests.user_id
  ),
  --Step 6:Payment
  payment as (
    SELECT DISTINCT
      ride_requests.user_id,
    COUNT(ride_requests.ride_id) AS ride_count
    FROM
      ride_requests
      JOIN transactions ON ride_requests.ride_id = transactions.ride_id
    WHERE
      transactions.charge_status = 'Approved'
    GROUP BY ride_requests.user_id
  ),
  --Step 7:Review
  review_cte as (
    SELECT DISTINCT
      reviews.user_id,
    COUNT(ride_id) AS ride_count
    
    FROM
      reviews
      JOIN payment ON reviews.user_id = payment.user_id
    GROUP BY reviews.user_id
  ),
 
 -- Combining output of each CTE above--
analysis as (
  SELECT
    1 AS funnel_step,
    'App Download' AS Stage,
    COUNT(*) AS user_count,
    0 AS ride_count
  FROM
    download AS Values
  UNION ALL
  SELECT
    2 AS funnel_step,
    'Sign Up' AS Stage,
    COUNT(user_id) AS user_count,
    0 AS ride_count
  FROM
    signups AS Values
  UNION ALL
  SELECT
    3 AS funnel_step,
    'Request Ride' AS Stage,
    COUNT(user_id) AS user_count,
    SUM(ride_count) AS ride_count
  FROM
    request_ride_cte AS Values
  UNION ALL
  SELECT
    4 AS funnel_step,
    'Driver Acceptance' AS Stage,
    COUNT(user_id) AS user_count,
    SUM(ride_count) AS ride_count
  FROM
    driveracceptance AS Values
  UNION ALL
  SELECT
    5 AS funnel_step,
    'Ride' AS Stage,
    COUNT(user_id) AS user_count,
    SUM(ride_count) AS ride_count
  FROM
    ride AS Values
  UNION ALL
  SELECT
    6 AS funnel_step,
    'Payment' AS Stage,
    COUNT(user_id) AS user_count,
    SUM(ride_count) AS ride_count
  FROM
    payment AS Values
  UNION ALL
  SELECT
    7 AS funnel_step,
    'Review' AS Stage,
    COUNT(user_id) AS user_count,
    SUM(ride_count) AS ride_count
  FROM
    review_cte AS Values
  ORDER BY
    funnel_step ASC
)
  
SELECT
  funnel_step,
  Stage,
  user_count,
  lag(user_count, 1) OVER (ORDER BY funnel_step) AS previous_user_count,
  CASE
    WHEN lag(user_count, 1) OVER (ORDER BY funnel_step) = 0 THEN NULL
    ELSE ROUND((1.0 - user_count::NUMERIC / lag(user_count, 1) OVER (ORDER BY funnel_step)), 2)
  END AS user_drop_off_rate,
  ride_count,
  lag(ride_count, 1) OVER (ORDER BY funnel_step) AS previous_ride_count,
  CASE
    WHEN lag(ride_count, 1) OVER (ORDER BY funnel_step) = 0 THEN NULL
    ELSE ROUND((1.0 - ride_count::NUMERIC / lag(ride_count, 1) OVER (ORDER BY funnel_step)), 2)
  END AS ride_drop_off_rate
FROM
  analysis;








