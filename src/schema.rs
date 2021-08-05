table! {
    backfill_completions (id) {
        id -> Uuid,
        contract_address -> Varchar,
        event_name -> Varchar,
    }
}

table! {
    claims (id) {
        id -> Uuid,
        transaction_hash -> Varchar,
        event_sequence -> Int4,
        block_height -> Int4,
        block_timestamp -> Timestamp,
        initiator_address -> Varchar,
        distributor_address -> Varchar,
        epoch_number -> Int4,
    }
}

table! {
    distributions (id) {
        id -> Uuid,
        distributor_address -> Varchar,
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
        token_amount -> Numeric,
        zil_amount -> Numeric,
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

table! {
    pool_txs (id) {
        id -> Uuid,
        transaction_hash -> Varchar,
        block_height -> Int4,
        block_timestamp -> Timestamp,
        initiator_address -> Varchar,
        token_address -> Varchar,

        token_amount -> Nullable<Numeric>,
        zil_amount -> Nullable<Numeric>,

        tx_type -> Varchar,

        swap0_is_sending_zil -> Nullable<Bool>,

        swap1_token_address -> Nullable<Varchar>,
        swap1_token_amount -> Nullable<Numeric>,
        swap1_zil_amount -> Nullable<Numeric>,
        swap1_is_sending_zil -> Nullable<Bool>,

        change_amount -> Nullable<Numeric>,
    }
}

allow_tables_to_appear_in_same_query!(
    backfill_completions,
    claims,
    distributions,
    liquidity_changes,
    swaps,
);
