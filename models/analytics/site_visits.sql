SELECT
    gen_random_uuid() as id,
    0::bigint as timestamp,
    ''::varchar as session_id,
    ''::varchar as app 
WHERE FALSE