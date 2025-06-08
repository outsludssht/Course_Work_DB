CREATE TABLE IF NOT EXISTS tracks (
    track_id VARCHAR(50) PRIMARY KEY,
    song_title VARCHAR(200),
    popularity INTEGER,
    duration_ms INTEGER
);

CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY,
    artist_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS albums (
    album_id VARCHAR(50) PRIMARY KEY,
    album_title VARCHAR(200),
    release_date DATE
);

CREATE TABLE IF NOT EXISTS genres (
    genre_name VARCHAR(100) PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS locations (
    location_id INTEGER PRIMARY KEY,
    country VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100),
    gender CHAR(1),
    birthdate DATE,
    location_id INTEGER,
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

CREATE TABLE IF NOT EXISTS track_artists (
    track_id VARCHAR(50),
    artist_id VARCHAR(50),
    PRIMARY KEY (track_id, artist_id),
    FOREIGN KEY (track_id) REFERENCES tracks(track_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);

CREATE TABLE IF NOT EXISTS track_albums (
    track_id VARCHAR(50),
    album_id VARCHAR(50),
    PRIMARY KEY (track_id, album_id),
    FOREIGN KEY (track_id) REFERENCES tracks(track_id),
    FOREIGN KEY (album_id) REFERENCES albums(album_id)
);

CREATE TABLE IF NOT EXISTS artist_genres (
    artist_id VARCHAR(50),
    genre_id VARCHAR(50),
    PRIMARY KEY (artist_id, genre_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_name)
);

CREATE TABLE IF NOT EXISTS listening_sessions (
    session_id VARCHAR(100),
    user_id VARCHAR(50),
    track_id VARCHAR(50),
    listen_duration INTEGER,
    start_session_time TIMESTAMP,
    device VARCHAR(50),
    location_id INTEGER,
    PRIMARY KEY (session_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (track_id) REFERENCES tracks(track_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

select * from listening_sessions;




-------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS temp_tracks_albums_artists (
    track_id VARCHAR(50) PRIMARY KEY,
    song_title VARCHAR(100),
    album_id VARCHAR(50),
    release_date_x TEXT,
    popularity INT,
    duration_ms INT,
    album_title VARCHAR(100),
    artist_id VARCHAR(50),
    release_date_y TEXT,
    artist_name VARCHAR(100),
    genres VARCHAR(100)
);

copy temp_tracks_albums_artists from 'c:\csv\tracks_albums_artists.csv' DELIMITER ',' CSV HEADER;
select * from temp_tracks_albums_artists;

CREATE TABLE IF NOT EXISTS temp_users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100),
    gender CHAR(1),
    birthdate DATE,
    location INT
);

copy temp_users from 'c:\csv\users.csv' DELIMITER ',' CSV HEADER;
select * from temp_users;

CREATE TABLE IF NOT EXISTS temp_generated_sessions (
    session_id INT PRIMARY KEY,
    user_id VARCHAR(50),
    start_session_time TIMESTAMP,
    device VARCHAR(50),
    track_id VARCHAR(50),
    listen_duration INT,
    location_id INT
);

copy temp_generated_sessions from 'c:\csv\generated_sessions.csv' DELIMITER ',' CSV HEADER;
select * from temp_generated_sessions;

CREATE TABLE IF NOT EXISTS temp_locations (
    location_id INT PRIMARY KEY,
    country VARCHAR(50)
);

copy temp_locations from 'c:\csv\locations.csv' DELIMITER ',' CSV HEADER;
select * from temp_locations;

------------------------------------------------------------------------------
--tracks
INSERT INTO tracks (track_id, song_title, popularity, duration_ms)
SELECT DISTINCT track_id, song_title, popularity, duration_ms
FROM temp_tracks_albums_artists
ON CONFLICT (track_id) DO NOTHING;

--artists
INSERT INTO artists (artist_id, artist_name)
SELECT DISTINCT artist_id, artist_name
FROM temp_tracks_albums_artists
ON CONFLICT (artist_id) DO NOTHING;

--albums
INSERT INTO albums (album_id, album_title, release_date)
SELECT DISTINCT
    album_id,
    album_title,
    CASE
        WHEN release_date_y ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(release_date_y, 'YYYY-MM-DD')
        WHEN release_date_y ~ '^\d{4}$' THEN TO_DATE(release_date_y || '-01-01', 'YYYY-MM-DD')
        ELSE NULL
    END AS release_date
FROM temp_tracks_albums_artists
ON CONFLICT (album_id) DO NOTHING;

--genres
INSERT INTO genres (genre_name)
SELECT DISTINCT TRIM(genre_name)
FROM (
    SELECT unnest(string_to_array(genres, ',')) AS genre_name
    FROM temp_tracks_albums_artists
    WHERE genres IS NOT NULL AND TRIM(genres) <> ''
) AS genre_list
WHERE TRIM(genre_name) <> ''
ON CONFLICT DO NOTHING;

--artist-genres
INSERT INTO artist_genres (artist_id, genre_id)
SELECT DISTINCT
    artist_id,
    TRIM(genre_name) AS genre_id
FROM (
    SELECT artist_id, unnest(string_to_array(genres, ',')) AS genre_name
    FROM temp_tracks_albums_artists
    WHERE genres IS NOT NULL AND TRIM(genres) <> ''
) AS artist_genre_list
WHERE TRIM(genre_name) <> ''
ON CONFLICT (artist_id, genre_id) DO NOTHING;

select * from artist_genres 
order by artist_id;

--track-artists
INSERT INTO track_artists (track_id, artist_id)
SELECT DISTINCT track_id, artist_id
FROM temp_tracks_albums_artists
ON CONFLICT (track_id, artist_id) DO NOTHING;

select * from track_artists;

--track_albums
INSERT INTO track_albums (track_id, album_id)
SELECT DISTINCT track_id, album_id
FROM temp_tracks_albums_artists
ON CONFLICT (track_id, album_id) DO NOTHING;

select * from track_albums;

--location
INSERT INTO locations (location_id, country)
SELECT DISTINCT location_id, country
FROM temp_locations
ON CONFLICT (location_id) DO NOTHING;

--users
INSERT INTO users (user_id, username, gender, birthdate, location_id)
SELECT user_id, username, gender, birthdate, location
FROM temp_users
ON CONFLICT (user_id) DO NOTHING;

--listening sessions
INSERT INTO listening_sessions (
    session_id, user_id, track_id, listen_duration,
    start_session_time, device, location_id
)
SELECT session_id, user_id, track_id, listen_duration,
       start_session_time, device, location_id
FROM temp_generated_sessions
ON CONFLICT (session_id) DO NOTHING;

select * from listening_sessions
order by start_session_time asc;

--очистка темп
TRUNCATE TABLE temp_tracks_albums_artists, temp_users, temp_generated_sessions, temp_locations;





