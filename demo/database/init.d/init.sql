CREATE USER 'utente'@'%' identified by 'password';

GRANT ALL PRIVILEGES ON *.* TO 'utente';

FLUSH PRIVILEGES;

DROP TABLE IF EXISTS import_data;

CREATE TABLE import_data (
    id INTEGER NOT NULL AUTO_INCREMENT,
    content VARCHAR(200),
    import_date DATETIME,
    PRIMARY KEY (id)
);