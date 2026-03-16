CREATE KEYSPACE IF NOT EXISTS market_data_logs
WITH replication = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
};

USE market_data_logs;

CREATE TABLE IF NOT EXISTS generic_logs (
    level text,
    ts timeuuid,
    message text,
    target text,
    location text,
    PRIMARY KEY ((level), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 21600;

CREATE TABLE IF NOT EXISTS component_exchange_logs (
    level text,
    component text,
    exchange text,
    ts timeuuid,
    message text,
    target text,
    location text,
    PRIMARY KEY ((level, component, exchange), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 21600;

CREATE TABLE IF NOT EXISTS instrument_logs (
    level text,
    instrument text,
    component text,
    exchange text,
    ts timeuuid,
    message text,
    target text,
    location text,
    PRIMARY KEY ((exchange, instrument), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 21600;
