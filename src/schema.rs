table! {
    swaps (id) {
        id -> Int8,
        transaction_hash -> Varchar,
        event_sequence -> Int4,
        block_height -> Int4,
        block_timestamp -> Timestamp,
        initiator_address -> Varchar,
        token_address -> Varchar,
        token_amount -> Numeric,
        zil_amount -> Numeric,
        is_sending_zil -> Bool,
    }
}
