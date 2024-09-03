CREATE VIEW recent_games_6months AS
SELECT *
FROM chess_games
WHERE date >= (CURRENT_DATE - INTERVAL '6 months');

-- Create a view for recent games where the specified player is white
CREATE OR REPLACE VIEW recent_white_games_6months AS
SELECT *
FROM chess_games
WHERE date >= (CURRENT_DATE - INTERVAL '6 months') AND white_player = '{player_name}';

-- Create a view for recent games where the specified player is black
CREATE OR REPLACE VIEW recent_black_games_6months AS
SELECT *
FROM chess_games
WHERE date >= (CURRENT_DATE - INTERVAL '6 months') AND black_player = '{player_name}';


