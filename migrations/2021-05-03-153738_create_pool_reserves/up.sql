CREATE VIEW pool_reserves AS
SELECT 
	token_address,
	SUM(zil_amount) zil_amount,
	SUM(token_amount) token_amount
FROM (
SELECT
	token_address,
	-- is_sending_zil = pool receives ZIL
	SUM(CASE 
		WHEN is_sending_zil = true THEN zil_amount
		ELSE -zil_amount
	END) zil_amount,
	SUM(CASE 
		WHEN is_sending_zil = true THEN -token_amount
		ELSE token_amount
	END) token_amount
FROM swaps
GROUP BY 
	token_address

UNION
SELECT
	token_address,
	SUM(SIGN(change_amount) * zil_amount),
	SUM(SIGN(change_amount) * token_amount)
FROM liquidity_changes
GROUP BY 
	token_address
) subquery
GROUP BY token_address;
