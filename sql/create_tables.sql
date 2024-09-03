CREATE TABLE chess_games (
    id SERIAL PRIMARY KEY,
    event VARCHAR(255),
    site VARCHAR(255),
    date DATE,
    round VARCHAR(50),
    white_player VARCHAR(255),
    black_player VARCHAR(255),
    result VARCHAR(10),
    white_elo INTEGER,
    black_elo INTEGER,
    time_control VARCHAR(50),
    end_time VARCHAR(50),
    termination VARCHAR(255),
    moves TEXT
);
