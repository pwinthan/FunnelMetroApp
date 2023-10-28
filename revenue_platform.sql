SELECT
    app_downloads.platform,
    SUM(transactions.purchase_amount_usd) AS total_revenue
FROM
    app_downloads
JOIN
    signups ON app_downloads.app_download_key = signups.session_id
LEFT JOIN
    ride_requests ON signups.user_id = ride_requests.user_id
LEFT JOIN
    transactions ON ride_requests.ride_id = transactions.ride_id
GROUP BY
    app_downloads.platform;
