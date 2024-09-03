CREATE VIEW recent_games_6months AS
SELECT *
FROM chess_games
WHERE date >= (CURRENT_DATE - INTERVAL '6 months');

CREATE VIEW recent_white_games_6months AS
SELECT *
FROM chess_games
WHERE date >= (CURRENT_DATE - INTERVAL '6 months') AND white_player='gabrielpizzo';

CREATE VIEW recent_black_games_6months AS
SELECT *
FROM chess_games
WHERE date >= (CURRENT_DATE - INTERVAL '6 months') AND black_player='gabrielpizzo';

