-- This file should undo anything in `up.sql`

-- update 2 smart contract addresses replaced in state migration
UPDATE liquidity_changes
	SET initiator_address='zil1zep7ypnuah8gejfyapqv5mr7m8yp3lfv3mmk7c'
WHERE initiator_address='zil12t5yy6xwadasjmn40skfxq59q3wjagxd8sk9g3';

UPDATE liquidity_changes
	SET initiator_address='zil1j02fmhfzmlpcx8fx89dr7r8thh584vd4aee0wx'
WHERE initiator_address='zil1nvsh5cflvamgsvmx2t3rqdz2jl8x7x9k7fqc6w';
