table! {
    backfill_completions (id) {
        id -> Uuid,
        contract_address -> Varchar,
        event_name -> Varchar,
    }
}

table! {
    block_syncs (id) {
        id -> Uuid,
        block_height -> Int4,
        block_timestamp -> Timestamp,
        num_txs -> Int4,
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
        amount -> Numeric,
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
        router_address -> Varchar,
        pool_address -> Varchar,
        amount_0 -> Numeric,
        amount_1 -> Numeric,
        liquidity -> Numeric,
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
        pool_address -> Varchar,
        router_address -> Varchar,
        to_address -> Varchar,
        amount_0_in -> Numeric,
        amount_1_in -> Numeric,
        amount_0_out -> Numeric,
        amount_1_out -> Numeric,
    }
}

table! {
    pool_txs (id) {
        id -> Uuid,
        transaction_hash -> Varchar,
        block_height -> Int4,
        block_timestamp -> Timestamp,
        initiator_address -> Varchar,
        pool_address -> Varchar,
        router_address -> Varchar,
        to_address -> Nullable<Varchar>,

        amount_0 -> Nullable<Numeric>,
        amount_1 -> Nullable<Numeric>,
        liquidity -> Nullable<Numeric>,

        tx_type -> Varchar,

        amount_0_in -> Nullable<Numeric>,
        amount_1_in -> Nullable<Numeric>,
        amount_0_out -> Nullable<Numeric>,
        amount_1_out -> Nullable<Numeric>,
    }
}

allow_tables_to_appear_in_same_query!(
    backfill_completions,
    block_syncs,
    claims,
    distributions,
    liquidity_changes,
    swaps,
);
