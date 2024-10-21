class SqlQueries:

    # CREATE TABLES

    songplay_table_create = """
        CREATE TABLE IF NOT EXISTS songplays
        (
            songplay_id         INTEGER IDENTITY(0,1) NOT NULL SORTKEY,
            start_time          TIMESTAMP NOT NULL,
            user_id             INTEGER NOT NULL DISTKEY,
            level               VARCHAR(30) NOT NULL,
            song_id             VARCHAR(18),
            artist_id           VARCHAR(18),
            session_id          INTEGER NOT NULL,
            location            VARCHAR(50),
            user_agent          VARCHAR(200)
        )
        """

    user_table_create = """
        CREATE TABLE IF NOT EXISTS users 
        (
            user_id             INTEGER NOT NULL DISTKEY,
            first_name          VARCHAR(30),
            last_name           VARCHAR(30),
            gender              VARCHAR(1),
            level               VARCHAR(30) NOT NULL
        );
        """

    song_table_create = """
        CREATE TABLE IF NOT EXISTS songs 
        (
            song_id            VARCHAR(18) NOT NULL DISTKEY,
            title              VARCHAR(500),
            artist_id          VARCHAR(18),
            year               INTEGER,
            duration           FLOAT
        );
        """

    artist_table_create = """
        CREATE TABLE IF NOT EXISTS artists
        (
            artist_id            VARCHAR(18) NOT NULL DISTKEY,
            name                 VARCHAR(500),
            location             VARCHAR(500),
            latitude             DECIMAL(8,6),
            longitude            DECIMAL(9,6)
        );
        """

    time_table_create = """
        CREATE TABLE IF NOT EXISTS time 
        (
            start_time           TIMESTAMP NOT NULL SORTKEY,
            hour                 SMALLINT, 
            day                  SMALLINT,
            week                 SMALLINT,
            month                SMALLINT,
            year                 SMALLINT,
            weekday              SMALLINT
        );
        """

    staging_events_table_create = """
        CREATE TABLE IF NOT EXISTS staging_events
        (
            artist               VARCHAR(200),
            auth                 VARCHAR(30) NOT NULL,
            firstName            VARCHAR(30),
            gender               VARCHAR(1),
            itemInSession        INTEGER NOT NULL,
            lastName             VARCHAR(30),
            length               FLOAT,
            level                VARCHAR(30) NOT NULL,
            location             VARCHAR(50),
            method               VARCHAR(30) NOT NULL,
            page                 VARCHAR(30) NOT NULL,
            registration         FLOAT,
            sessionId            INTEGER NOT NULL,
            song                 VARCHAR(200),
            status               INTEGER NOT NULL,
            ts                   BIGINT NOT NULL,
            userAgent            VARCHAR(200),
            userId               INTEGER
        );
    """

    staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs 
        (
            song_id              VARCHAR(18),
            num_songs            INTEGER,
            title                VARCHAR(500),
            artist_name          VARCHAR(500),
            artist_latitude      DECIMAL(8,6),
            year                 INTEGER,
            duration             FLOAT,
            artist_id            VARCHAR(18),
            artist_longitude     DECIMAL(9,6),
            artist_location      VARCHAR(500)
        );
    """

    # LOAD TABLES

    songplay_table_insert = """
        INSERT INTO songplays(
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent)
        SELECT DISTINCT 
            TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            se.userId AS user_id,
            se.level AS level,
            ss.song_id AS song_id,
            ss.artist_id AS artist_id,
            se.sessionId AS session_id,
            se.location AS location,
            se.userAgent AS user_agent
        FROM staging_events AS se
        LEFT OUTER JOIN staging_songs AS ss ON (se.artist = ss.artist_name and se.song = ss.title)
        WHERE se.page = 'NextSong';
    """

    user_dim_table_insert = """
        INSERT INTO users(
            user_id,
            first_name,
            last_name,
            gender,
            level)
        SELECT DISTINCT 
            userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender as gender,
            level as level
        FROM staging_events
        WHERE page = 'NextSong';
    """

    song_dim_table_insert = """
        INSERT INTO songs(
            song_id,
            title,
            artist_id,
            year,
            duration)
        SELECT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs;
    """

    artist_dim_table_insert = """
        INSERT INTO artists(
            artist_id,
            name,
            location,
            latitude,
            longitude)
        SELECT DISTINCT
            artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
        FROM staging_songs;
    """

    time_dim_table_insert = """
        INSERT INTO time(
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday)
        SELECT 
            DISTINCT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS converted_ts,
            EXTRACT(hour FROM converted_ts) AS hour,
            EXTRACT(day FROM converted_ts) AS day,
            EXTRACT(week FROM converted_ts) AS week,
            EXTRACT(month FROM converted_ts) AS month,
            EXTRACT(year FROM converted_ts) AS year,
            EXTRACT(week FROM converted_ts) AS weekday
        FROM staging_events                
        WHERE page = 'NextSong';
    """

    # QUALITY CHECKS

    user_data_quality_check = """
        SELECT
            COUNT(CASE WHEN user_id IS NULL THEN 1 END) AS user_id_null_count,
            COUNT(CASE WHEN first_name IS NULL THEN 1 END) AS first_name_null_count,
            COUNT(CASE WHEN last_name IS NULL THEN 1 END) AS last_name_null_count,
            COUNT(CASE WHEN gender IS NULL THEN 1 END) AS email_null_count,
            COUNT(CASE WHEN level IS NULL THEN 1 END) AS age_null_count
        FROM
            users;
    """

    time_data_quality_check = """
        SELECT
            COUNT(CASE WHEN start_time IS NULL THEN 1 END) AS start_time_null_count,
            COUNT(CASE WHEN hour IS NULL THEN 1 END) AS hour_null_count,
            COUNT(CASE WHEN day IS NULL THEN 1 END) AS day_null_count,
            COUNT(CASE WHEN week IS NULL THEN 1 END) AS week_null_count,
            COUNT(CASE WHEN month IS NULL THEN 1 END) AS month_null_count,
            COUNT(CASE WHEN year IS NULL THEN 1 END) AS year_null_count,
            COUNT(CASE WHEN weekday IS NULL THEN 1 END) AS weekday_null_count
        FROM
            time;
    """

    songplays_data_quality_check = """
        SELECT
            COUNT(CASE WHEN songplay_id IS NULL THEN 1 END) AS songplay_id_null_count,
            COUNT(CASE WHEN start_time IS NULL THEN 1 END) AS start_time_null_count,
            COUNT(CASE WHEN user_id IS NULL THEN 1 END) AS user_id_null_count,
            COUNT(CASE WHEN level IS NULL THEN 1 END) AS level_null_count,
            -- nulls allowed
            -- COUNT(CASE WHEN song_id IS NULL THEN 1 END) AS song_id_null_count,
            -- COUNT(CASE WHEN artist_id IS NULL THEN 1 END) AS artist_id_null_count,
            COUNT(CASE WHEN session_id IS NULL THEN 1 END) AS session_id_null_count,
            COUNT(CASE WHEN location IS NULL THEN 1 END) AS location_null_count,
            COUNT(CASE WHEN user_agent IS NULL THEN 1 END) AS user_agent_null_count
        FROM songplays;
    """

    songs_data_quality_check = """
        SELECT
            COUNT(CASE WHEN song_id IS NULL THEN 1 END) AS song_id_null_count,
            COUNT(CASE WHEN title IS NULL THEN 1 END) AS title_null_count,
            COUNT(CASE WHEN artist_id IS NULL THEN 1 END) AS artist_id_null_count,
            COUNT(CASE WHEN year IS NULL THEN 1 END) AS year_null_count,
            COUNT(CASE WHEN duration IS NULL THEN 1 END) AS duration_null_count
        FROM songs;
    """

    artists_data_quality_check = """
        SELECT
            COUNT(CASE WHEN artist_id IS NULL THEN 1 END) AS artist_id_null_count,
            COUNT(CASE WHEN name IS NULL THEN 1 END) AS name_null_count,
            COUNT(CASE WHEN location IS NULL THEN 1 END) AS location_null_count
            -- nulls allowed
            -- COUNT(CASE WHEN latitude IS NULL THEN 1 END) AS latitude_null_count, 
            -- COUNT(CASE WHEN longitude IS NULL THEN 1 END) AS longitude_null_count, 
        FROM artists;
    """
