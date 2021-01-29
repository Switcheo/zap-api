table! {
    distributions (id) {
        id -> Uuid,
        epoch_number -> Int4,
        address_bech32 -> Varchar,
        address_hex -> Varchar,
        amount -> Numeric,
        proof -> Varchar,
    }
}

table! {
    liquidity_changes (id) {
        id -> Uuid,
        transaction_hash -> Varchar,
        event_sequence -> Int4,
        block_height -> Int4,
        block_timestamp -> Timestamp,
        initiator_address -> Varchar,
        token_address -> Varchar,
        change_amount -> Numeric,
    }
}

table! {
    swaps (id) {
        id -> Uuid,
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

allow_tables_to_appear_in_same_query!(
    distributions,
    liquidity_changes,
    swaps,
);
