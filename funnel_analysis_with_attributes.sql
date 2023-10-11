 -- Step 1: User app download
with
  download as (
    select distinct
      app_download_key,
      platform,
      age_range,
      download_ts
    from
      app_downloads
      JOIN signups ON signups.session_id = app_downloads.app_download_key
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
      rr.user_id,
      COUNT(rr.ride_id) AS ride_count,
      platform,
      age_range,
      download_ts
    FROM
      ride_requests rr
      JOIN signups s ON rr.user_id = s.user_id
      JOIN app_downloads ON s.session_id = app_downloads.app_download_key
    GROUP BY
      rr.user_id,
      platform,
      s.age_range,
      app_downloads.download_ts
  ),
  --Step 4: Driver Acceptance 
  driveracceptance as (
    SELECT DISTINCT
      rr.user_id,
      COUNT(rr.ride_id) AS ride_count,
      app_downloads.platform,
      s.age_range,
      app_downloads.download_ts
    FROM
      ride_requests rr
      JOIN request_ride_cte rrc ON rr.user_id = rrc.user_id
      JOIN signups s ON rr.user_id = s.user_id
      JOIN app_downloads ON s.session_id = app_downloads.app_download_key
    WHERE
      rr.accept_ts IS NOT NULL
    GROUP BY
      rr.user_id,
      app_downloads.platform,
      s.age_range,
      app_downloads.download_ts
  ),
  --Step 5: Ride 
  ride as (
    SELECT DISTINCT
      rr.user_id,
      COUNT(rr.ride_id) AS ride_count,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
    FROM
      ride_requests rr
      JOIN driveracceptance da ON rr.user_id = da.user_id
      JOIN signups ON rr.user_id = signups.user_id
      JOIN app_downloads ON signups.session_id = app_downloads.app_download_key
    WHERE
      rr.dropoff_ts IS NOT NULL
    GROUP BY
      rr.user_id,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
  ),
  --Step 6: Payment
  payment as (
    SELECT DISTINCT
      rr.user_id,
      COUNT(rr.ride_id) AS ride_count,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
    FROM
      ride_requests rr
      JOIN transactions t ON rr.ride_id = t.ride_id
      JOIN signups ON rr.user_id = signups.user_id
      JOIN app_downloads ON signups.session_id = app_downloads.app_download_key
    WHERE
      t.charge_status = 'Approved'
    GROUP BY
      rr.user_id,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
  ),
  --Step 7: Review
  review_cte as (
    SELECT DISTINCT
      r.user_id,
      COUNT(r.ride_id) AS ride_count,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
    FROM
      reviews r
      JOIN payment p ON r.user_id = p.user_id
      JOIN signups ON r.user_id = signups.user_id
      JOIN app_downloads ON signups.session_id = app_downloads.app_download_key
    GROUP BY
      r.user_id,
      app_downloads.platform,
      signups.age_range,
      app_downloads.download_ts
  ),
  -- Combining output of each CTE above--
  analysis as (
    SELECT
      1 AS funnel_step,
      'App Download' AS Stage,
      COUNT(*) AS user_count,
      0 AS ride_count,
      platform,
      age_range,
      CAST(download_ts AS DATE) --casting date to remove time (hh:mm:ss) for grouping by date
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
      NULL AS platform,
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
      driveracceptance AS
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
      ride AS
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
      payment AS
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
      payment AS
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
  user_count,
  platform,
  age_range,
  CAST(download_ts AS DATE),
  ride_count
FROM
  analysis;