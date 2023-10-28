-- Objective: To understand the user journey through Metrocar's platform and identify areas for improvement and optimization

-- We are looking at these metrics from a user-level; the number of [distinct] users that signed up, requested a ride and completed. A user can request multiple rides but will only be counted once.

-- Funnel Steps: App Download, Signup, Request Ride, Driver Acceptance, Ride, Payment, Review

-- Step 1: User app download
with
  download as (
    select
      app_download_key,
      platform,
      age_range,
      download_ts
    from
      app_downloads
      JOIN signups ON app_downloads.app_download_key = signups.session_id
  ),
  --Step 2: Signups 
  signups_cte as (
    SELECT DISTINCT
      user_id,
      platform,
      age_range,
      download_ts
    FROM
      signups
      JOIN app_downloads ON signups.session_id = app_downloads.app_download_key
  ),
  --Step 3: Request Ride (from the signup above)
  request_ride_cte as (
    SELECT DISTINCT
      ride_requests.user_id,
      COUNT(ride_requests.ride_id) AS ride_count,
      platform,
      age_range,
      download_ts
    FROM
      ride_requests
      JOIN signups ON ride_requests.user_id = signups.user_id
      JOIN app_downloads ON app_downloads.app_download_key = signups.session_id
    GROUP BY
      ride_requests.user_id,
      platform,
      signups.age_range,
      download_ts
  ),
  --Step 4: Driver Acceptance (from ride_request_users_above)
  driveracceptance as (
    SELECT DISTINCT
      ride_requests.user_id,
      COUNT(ride_requests.ride_id) AS ride_count,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
    FROM
      ride_requests
      --JOIN request_ride_cte ON ride_requests.user_id = request_ride_cte.user_id
      JOIN signups ON ride_requests.user_id = signups.user_id
      JOIN app_downloads ON app_downloads.app_download_key = signups.session_id
    WHERE
      ride_requests.accept_ts IS NOT NULL
    GROUP BY
      ride_requests.user_id,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
  ),
  --Step 5: Ride (from the users above) 
  ride as (
    SELECT DISTINCT
      ride_requests.user_id,
      COUNT(ride_requests.ride_id) AS ride_count,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
    FROM
      ride_requests
      JOIN signups ON ride_requests.user_id = signups.user_id
      JOIN app_downloads ON signups.session_id = app_downloads.app_download_key
    WHERE
      ride_requests.dropoff_ts IS NOT NULL
    GROUP BY
      ride_requests.user_id,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
  ),
  --Step 6: Payment (from the users above)
  payment_cte as (
    SELECT DISTINCT
      ride_requests.user_id,
      COUNT(transactions.ride_id) AS ride_count,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
    FROM
      transactions
      JOIN ride_requests ON ride_requests.ride_id = transactions.ride_id
      JOIN signups ON ride_requests.user_id = signups.user_id
      JOIN app_downloads ON signups.session_id = app_downloads.app_download_key
    WHERE
      transactions.charge_status = 'Approved'
    GROUP BY
      ride_requests.user_id,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
  ),
  --Step 7: Review (from the users above)
  review_cte as (
    SELECT DISTINCT
      reviews.user_id,
      COUNT(reviews.ride_id) AS ride_count,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
    FROM
      reviews
      --JOIN payment_cte ON  payment_cte.user_id = reviews.user_id 
      JOIN signups ON reviews.user_id = signups.user_id
      JOIN ride_requests ON reviews.ride_id = ride_requests.ride_id
      JOIN app_downloads ON signups.session_id = app_downloads.app_download_key
      JOIN transactions ON transactions.ride_id = reviews.ride_id
    GROUP BY
      reviews.user_id,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
  ),
  -- Combining output of each CTE above--
  funnel_analysis as (
    SELECT
      1 AS funnel_step,
      'App Download' AS Stage,
      COUNT(*) AS user_count,
      0 AS ride_count,
      platform,
      age_range,
      CAST(download_ts AS DATE) --casting download_ts to DATE format to remove time (hh:mm:ss)
    FROM
      download AS
    Values
    GROUP BY
      platform,
      age_range,
      CAST(download_ts AS DATE)
    UNION ALL
    SELECT
      2 AS funnel_step,
      'Sign Up' AS Stage,
      COUNT(user_id) AS user_count,
      0 AS ride_count,
      platform,
      age_range,
      CAST(download_ts AS DATE)
    FROM
      signups_cte AS
    Values
    GROUP BY
      platform,
      age_range,
      CAST(download_ts AS DATE)
    UNION ALL
    SELECT
      3 AS funnel_step,
      'Request Ride' AS Stage,
      COUNT(user_id) AS user_count,
      SUM(ride_count) AS ride_count,
      platform,
      age_range,
      CAST(download_ts AS DATE)
    FROM
      request_ride_cte AS
    Values
    GROUP BY
      platform,
      age_range,
      CAST(download_ts AS DATE)
    UNION ALL
    SELECT
      4 AS funnel_step,
      'Driver Acceptance' AS Stage,
      COUNT(user_id) AS user_count,
      SUM(ride_count) AS ride_count,
      platform,
      age_range,
      CAST(download_ts AS DATE)
    FROM
      driveracceptance AS
    Values
    GROUP BY
      platform,
      age_range,
      CAST(download_ts AS DATE)
    UNION ALL
    SELECT
      5 AS funnel_step,
      'Ride' AS Stage,
      COUNT(user_id) AS user_count,
      SUM(ride_count) AS ride_count,
      platform,
      age_range,
      CAST(download_ts AS DATE)
    FROM
      ride AS
    Values
    GROUP BY
      platform,
      age_range,
      CAST(download_ts AS DATE)
    UNION ALL
    SELECT
      6 AS funnel_step,
      'Payment' AS Stage,
      COUNT(user_id) AS user_count,
      SUM(ride_count) AS ride_count,
      platform,
      age_range,
      CAST(download_ts AS DATE)
    FROM
      payment_cte AS
    Values
    GROUP BY
      platform,
      age_range,
      CAST(download_ts AS DATE)
    UNION ALL
    SELECT
      7 AS funnel_step,
      'Review' AS Stage,
      COUNT(user_id) AS user_count,
      SUM(ride_count) AS ride_count,
      platform,
      age_range,
      CAST(download_ts AS DATE)
    FROM
      review_cte AS
    Values
    GROUP BY
      platform,
      age_range,
      CAST(download_ts AS DATE)
    ORDER BY
      funnel_step,
      age_range,
      download_ts ASC
  )
SELECT
  funnel_step,
  Stage,
  age_range,
  download_ts,
  platform,
  SUM(user_count) AS user_count,
  SUM(ride_count) AS ride_count
FROM
  funnel_analysis
GROUP BY
  funnel_step,
  Stage,
  age_range,
  download_ts,
  platform
ORDER BY
  funnel_step,
  age_range,
  download_ts,
  platform;
