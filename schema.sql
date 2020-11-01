CREATE TABLE checks (
    url character varying(1024) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    date date DEFAULT CURRENT_DATE NOT NULL,
    error character varying(1024),
    status_code integer,
    response_time double precision,
    text character varying(1024)
) PARTITION BY RANGE (date);
