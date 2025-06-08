--Сколько раз каждый трек был прослушан по дням?
--oltp
SELECT 
    ls.track_id,
    DATE(ls.start_session_time) AS listen_date,
    COUNT(*) AS listen_count
FROM listening_sessions ls
GROUP BY ls.track_id, listen_date
ORDER BY listen_date, listen_count DESC;

--olap
SELECT 
    fte.track_id,
    dd.year,
    dd.month,
    dd.day,
    fte.total_listens
FROM fact_track_engagement fte
JOIN dim_date dd ON fte.date_key = dd.date_key
ORDER BY dd.year, dd.month, dd.day;

--Какие устройства чаще всего используются для прослушивания?
--oltp
SELECT 
    device,
    COUNT(*) AS usage_count
FROM listening_sessions
GROUP BY device
ORDER BY usage_count DESC;

--olap
SELECT 
    dd.device_type,
    COUNT(*) AS usage_count
FROM fact_listens fl
JOIN dim_device dd ON fl.device_id = dd.device_id
GROUP BY dd.device_type
ORDER BY usage_count DESC;

--Какие жанры наиболее популярны по общему времени прослушивания?
--oltp
SELECT 
    ag.genre_id AS genre_name,
    SUM(ls.listen_duration) AS total_duration
FROM listening_sessions ls
JOIN tracks t ON ls.track_id = t.track_id
JOIN track_artists ta ON t.track_id = ta.track_id
JOIN artist_genres ag ON ta.artist_id = ag.artist_id
GROUP BY ag.genre_id
ORDER BY total_duration DESC;

--olap
SELECT 
    dg.genre_name,
    SUM(fl.listen_duration) AS total_duration
FROM fact_listens fl
JOIN dim_track dt ON fl.track_id = dt.track_id
JOIN dim_album da ON dt.album_id = da.album_id
JOIN dim_artist dart ON da.artist_id = dart.artist_id
JOIN bridge_artist_genre bag ON dart.artist_id = bag.artist_id
JOIN dim_genre dg ON bag.genre_id = dg.genre_id
GROUP BY dg.genre_name
ORDER BY total_duration DESC;