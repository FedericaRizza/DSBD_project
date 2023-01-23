CREATE DATABASE IF NOT EXISTS prometheus_data;

USE prometheus_data;

CREATE TABLE IF NOT EXISTS datas (
    ID_metrica INT NOT NULL AUTO_INCREMENT,
    metric_name VARCHAR(16000) NOT NULL,
    slug VARCHAR(64) AS (SHA2 (metric_name, 256)) STORED NOT NULL,
    max_1h  float,
    max_3h  float,
    max_12h  float,
    min_1h  float,
    min_3h  float,
    min_12h  float,
    avg_1h  float,
    avg_3h  float,
    avg_12h  float,
    devstd_1h   float,
    devstd_3h   float,
    devstd_12h  float,
    max_predicted float DEFAULT NULL,
    min_predicted float DEFAULT NULL,
    avg_predicted   float DEFAULT NULL,
    autocorrelazione    float,
    stazionarieta   float,
    stagionalita    float,
    PRIMARY KEY (ID_metrica),
    UNIQUE (slug)
);


