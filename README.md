
CREATE DATABASE your_db_name;

-- Grant usage on schema public
GRANT USAGE ON SCHEMA public TO myuser;

-- Grant all privileges on the schema public
GRANT ALL PRIVILEGES ON SCHEMA public TO myuser;

-- If you need to grant all privileges on the database
GRANT ALL PRIVILEGES ON DATABASE mydatabase TO myuser;
