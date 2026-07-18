-- Unpack the raw JSONB payload faucet landed into typed columns.
-- faucet did the extract + load losslessly; dbt does the typing + shaping.
select
    data ->> 'id'                                as charge_id,
    (data ->> 'amount')::bigint                  as amount_cents,
    data ->> 'currency'                          as currency,
    data ->> 'status'                            as status,
    (data ->> 'paid')::boolean                   as paid,
    to_timestamp((data ->> 'created')::bigint)   as created_at
from {{ source('raw', 'charges_raw') }}
