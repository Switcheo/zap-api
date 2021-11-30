-- Your SQL goes here

-- remove white-hat exploit transactions
DELETE FROM liquidity_changes
	WHERE initiator_address='zil15p46v72tl4gvqn6d93u4zu8jvmdpzy7vf7ycsj'
	AND change_amount < 0
	AND block_height BETWEEN 1616134 AND 1616250;

-- update 2 smart contract addresses replaced in state migration
UPDATE liquidity_changes
	SET initiator_address='zil12t5yy6xwadasjmn40skfxq59q3wjagxd8sk9g3'
WHERE initiator_address='zil1zep7ypnuah8gejfyapqv5mr7m8yp3lfv3mmk7c';

UPDATE liquidity_changes
	SET initiator_address='zil1nvsh5cflvamgsvmx2t3rqdz2jl8x7x9k7fqc6w'
WHERE initiator_address='zil1j02fmhfzmlpcx8fx89dr7r8thh584vd4aee0wx';
