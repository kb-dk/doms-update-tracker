SET DATABASE SQL SYNTAX PGS TRUE;
DROP TABLE IF EXISTS updatetrackerlogs;
CREATE TABLE updatetrackerlogs
(
    key SERIAL PRIMARY KEY NOT NULL,
    pid VARCHAR(64) NOT NULL,
    happened TIMESTAMP WITH TIME ZONE NOT NULL,
    method VARCHAR(64) NOT NULL,
    param VARCHAR(255)
);
CREATE INDEX updatetrackerlogs_happened ON updatetrackerlogs (happened);
CREATE INDEX updatetrackerlogs_pid ON updatetrackerlogs (pid);

