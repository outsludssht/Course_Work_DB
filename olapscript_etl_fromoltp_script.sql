-- dim_date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

-- dim_user
CREATE TABLE IF NOT EXISTS dim_user (
    surrogate_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    username VARCHAR(50),
    gender CHAR(1),
    birthdate DATE,
    location_id INT,
    valid_from TIMESTAMP NOT NULL DEFAULT now(),
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);

-- dim_location
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT PRIMARY KEY,
    country VARCHAR(50)
);

-- dim_device
CREATE TABLE IF NOT EXISTS dim_device (
    device_id INT PRIMARY KEY,
    device_type VARCHAR(50)
);

-- dim_track
CREATE TABLE IF NOT EXISTS dim_track (
    track_id VARCHAR(50) PRIMARY KEY,
    song_title VARCHAR(200),
    popularity INT,
    duration_ms INT,
    album_id VARCHAR(50)
);

-- dim_album
CREATE TABLE IF NOT EXISTS dim_album (
    album_id VARCHAR(50) PRIMARY KEY,
    album_title VARCHAR(200),
    release_date DATE,
    artist_id VARCHAR(50),
    CONSTRAINT fk_artist_id FOREIGN KEY (artist_id) REFERENCES dim_artist(id)
);


-- dim_artist
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id VARCHAR(50) PRIMARY KEY,
    artist_name VARCHAR(100)
);

-- dim_genre
CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id VARCHAR(50) PRIMARY KEY,
    genre_name VARCHAR(100)
);

-- bridge_artist_genre
CREATE TABLE IF NOT EXISTS bridge_artist_genre (
    artist_id VARCHAR(50),
    genre_id VARCHAR(50),
    PRIMARY KEY (artist_id, genre_id),
    FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id),
    FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
);

-- fact_listens
CREATE SEQUENCE IF NOT EXISTS listen_id_seq;

CREATE TABLE IF NOT EXISTS fact_listens (
    listen_id BIGINT PRIMARY KEY DEFAULT nextval('listen_id_seq'),
    user_sk INT,
    track_id VARCHAR(50),
    date_key INT,
    device_id INT,
    location_id INT,
    listen_duration INT,
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (track_id) REFERENCES dim_track(track_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (device_id) REFERENCES dim_device(device_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);

-- fact_track_engagement
CREATE TABLE IF NOT EXISTS fact_track_engagement (
    track_id VARCHAR(50),
    date_key INT,
    total_listens INT,
    avg_listen_duration INT,
    total_listen_duration BIGINT,
    unique_users INT,
    track_age_in_2024 INT,
	location_id int,
	device_id int,
    PRIMARY KEY (track_id, date_key, location_id, device_id),
    FOREIGN KEY (track_id) REFERENCES dim_track(track_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
	FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
	FOREIGN KEY (device_id) REFERENCES dim_device(device_id)
);
--------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_foreign_server
        WHERE srvname = 'spotify_playlist_server'
    ) THEN
        CREATE SERVER spotify_playlist_server --сервер
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
            dbname 'spotify_playlist',
            host 'localhost',
            port '5432'
        );
    END IF;
END
$$;

ALTER SERVER spotify_playlist_server OPTIONS (SET dbname 'Spotify_playlist');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_user_mappings
        WHERE srvname = 'spotify_playlist_server'
        AND umuser = (SELECT usesysid FROM pg_user WHERE usename = CURRENT_USER)
    ) THEN
        CREATE USER MAPPING FOR CURRENT_USER --мапинг
        SERVER spotify_playlist_server
        OPTIONS (
            user 'postgres',
            password '1029384756'
        );
    END IF;
END
$$;

DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT foreign_table_name
        FROM information_schema.foreign_tables
        WHERE foreign_server_name = 'spotify_playlist_server'
    LOOP
        EXECUTE format('DROP FOREIGN TABLE IF EXISTS public.%I CASCADE;', rec.foreign_table_name);
    END LOOP;
END
$$;

IMPORT FOREIGN SCHEMA public
FROM SERVER spotify_playlist_server
INTO public;
--------------------------------------------------------------------------------------

-- 1. dim_artist
INSERT INTO dim_artist (artist_id, artist_name)
SELECT artist_id, artist_name
FROM artists
ON CONFLICT (artist_id) DO UPDATE
SET artist_name = EXCLUDED.artist_name;

select * from dim_artist;

-- 2. dim_genre
INSERT INTO dim_genre (genre_id, genre_name)
SELECT genre_name, genre_name
FROM genres
ON CONFLICT (genre_id) DO NOTHING;

-- 3. bridge_artist_genre
INSERT INTO bridge_artist_genre (artist_id, genre_id)
SELECT artist_id, genre_id
FROM artist_genres
ON CONFLICT (artist_id, genre_id) DO NOTHING;

select * from bridge_artist_genre;

-- 4. dim_album
INSERT INTO dim_album (album_id, album_title, release_date, artist_id)
SELECT 
    a.album_id,
    a.album_title,
    a.release_date,
    tart.artist_id
FROM albums AS a
JOIN track_albums AS ta ON a.album_id = ta.album_id
JOIN track_artists AS tart ON ta.track_id = tart.track_id
GROUP BY a.album_id, a.album_title, a.release_date, tart.artist_id
ON CONFLICT (album_id) DO UPDATE
SET album_title = EXCLUDED.album_title,
    release_date = EXCLUDED.release_date,
    artist_id = EXCLUDED.artist_id;

select * from dim_album;
select * from albums;

-- 5. dim_track
INSERT INTO dim_track (track_id, song_title, popularity, duration_ms, album_id)
SELECT 
    t.track_id,
    t.song_title,
    t.popularity,
    t.duration_ms,
    ta.album_id
FROM tracks as t
JOIN track_albums ta ON t.track_id = ta.track_id
ON CONFLICT (track_id) DO UPDATE
SET song_title = EXCLUDED.song_title,
    popularity = EXCLUDED.popularity,
    duration_ms = EXCLUDED.duration_ms,
    album_id = EXCLUDED.album_id;

-- 6. dim_location
INSERT INTO dim_location (location_id, country)
SELECT location_id, country
FROM locations
ON CONFLICT (location_id) DO UPDATE
SET country = EXCLUDED.country;

-- 7. dim_device
INSERT INTO dim_device (device_id, device_type)
SELECT 
    ROW_NUMBER() OVER (ORDER BY device_type) AS device_id, 
    device_type
FROM (
    SELECT DISTINCT device AS device_type
    FROM listening_sessions
) AS unique_devices
ON CONFLICT (device_id) DO NOTHING;


select * from dim_device;

-- 8. dim_date
INSERT INTO dim_date (date_key, year, month, day)
SELECT DISTINCT
    EXTRACT(YEAR FROM start_session_time)*10000 + EXTRACT(MONTH FROM start_session_time)*100 + EXTRACT(DAY FROM start_session_time) AS date_key,
    EXTRACT(YEAR FROM start_session_time)::INT,
    EXTRACT(MONTH FROM start_session_time)::INT,
    EXTRACT(DAY FROM start_session_time)::INT
FROM listening_sessions as ls
LEFT JOIN dim_date as dd
ON (EXTRACT(YEAR FROM start_session_time)*10000 + EXTRACT(MONTH FROM start_session_time)*100 + EXTRACT(DAY FROM start_session_time)) = dd.date_key
WHERE dd.date_key IS NULL;

select * from dim_date order by date_key;

-- 9. dim_user
UPDATE dim_user AS d
SET 
    valid_to = now(),
    is_current = FALSE
FROM users AS u
WHERE d.is_current = TRUE
  AND d.user_id = u.user_id
  AND (
        d.username IS DISTINCT FROM u.username OR
        d.gender IS DISTINCT FROM u.gender OR
        d.birthdate IS DISTINCT FROM u.birthdate OR
        d.location_id IS DISTINCT FROM u.location_id
  );

INSERT INTO dim_user (
    user_id, username, gender, birthdate, location_id, valid_from, valid_to, is_current
)
SELECT 
    u.user_id,
    u.username,
    u.gender,
    u.birthdate,
    u.location_id,
    now(),
    NULL,
    TRUE
FROM users AS u
LEFT JOIN dim_user AS d
    ON u.user_id = d.user_id AND d.is_current = TRUE
WHERE d.user_id IS NULL
   OR (
        u.username IS DISTINCT FROM d.username OR
        u.gender IS DISTINCT FROM d.gender OR
        u.birthdate IS DISTINCT FROM d.birthdate OR
        u.location_id IS DISTINCT FROM d.location_id
   );

UPDATE dim_user
SET valid_to = '2030-01-01 00:00:00'
WHERE valid_to IS NULL;

select * from dim_user;

-- 10. fact_listens
INSERT INTO fact_listens (
    listen_id,
    user_sk,
    track_id,
    date_key,
    device_id,
    location_id,
    listen_duration
)
SELECT 
    nextval('listen_id_seq') AS listen_id,
    u.surrogate_id,
    s.track_id,
    TO_NUMBER(TO_CHAR(s.start_session_time, 'YYYYMMDD'), '99999999') AS date_key,
    d.device_id,
    l.location_id,
    s.listen_duration
FROM listening_sessions as s
JOIN dim_user as u ON s.user_id = u.user_id AND u.is_current = TRUE
JOIN dim_device as d ON s.device = d.device_type
JOIN dim_location as l ON s.location_id = l.location_id
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_listens as f
    WHERE f.track_id = s.track_id
      AND f.user_sk = u.surrogate_id
      AND f.date_key = TO_NUMBER(TO_CHAR(s.start_session_time, 'YYYYMMDD'), '99999999')
      AND f.device_id = d.device_id
      AND f.location_id = l.location_id
      AND f.listen_duration = s.listen_duration
);

select * from fact_listens order by listen_duration asc;

-- 11. fact_track_engagement
INSERT INTO fact_track_engagement (
    track_id,
    date_key,
    total_listens,
    avg_listen_duration,
    total_listen_duration,
    unique_users,
    track_age_in_2024,
    location_id,
    device_id
)
SELECT
    ls.track_id,
    EXTRACT(YEAR FROM ls.start_session_time)*10000 + 
    EXTRACT(MONTH FROM ls.start_session_time)*100 + 
    EXTRACT(DAY FROM ls.start_session_time) AS date_key,
    COUNT(*) AS total_listens,
    AVG(ls.listen_duration)::INT AS avg_listen_duration,
    SUM(ls.listen_duration)::BIGINT AS total_listen_duration,
    COUNT(DISTINCT ls.user_id) AS unique_users,
    EXTRACT(YEAR FROM DATE '2024-01-01') - EXTRACT(YEAR FROM a.release_date) AS track_age_in_2024,
    ls.location_id,
    dd.device_id
FROM listening_sessions AS ls
JOIN dim_device AS dd ON dd.device_type = ls.device
JOIN tracks AS t ON ls.track_id = t.track_id
JOIN track_albums AS ta ON t.track_id = ta.track_id
JOIN albums AS a ON ta.album_id = a.album_id
GROUP BY ls.track_id, date_key, a.release_date, ls.location_id, dd.device_id
ON CONFLICT (track_id, date_key, location_id, device_id) DO UPDATE
SET total_listens = EXCLUDED.total_listens,
    avg_listen_duration = EXCLUDED.avg_listen_duration,
    total_listen_duration = EXCLUDED.total_listen_duration,
    unique_users = EXCLUDED.unique_users,
    track_age_in_2024 = EXCLUDED.track_age_in_2024;

select * from fact_track_engagement;

SELECT COUNT(DISTINCT track_id) AS unique_tracks_count
FROM fact_track_engagement;