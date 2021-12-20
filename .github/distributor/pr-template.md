## New Distributor Template

Please update the below configurations carefully to add your distributor to [config/config.yml](../../config/config.yml). Note that there are `testnet` and `mainnet` configurations in the same file, you should add the end of the `mainnet` configurations.

```yml
    - name:                                       # Your reward name, typically "<Token> Rewards"
      reward_token_symbol:                        # Rewarded token symbol/ticker
      reward_token_address_hex:                   # Rewarded token contract address (ByStr20)
      distributor_name:                           # Your team/entity name
      distributor_address_hex:                    # Your distributor contract address (ByStr20)
      developer_address:                          # Wallet address (Bech32) to receive developer 
                                                  # portion of each distribution
      emission_info:
        epoch_period:                             # Period per epoch in seconds (604800 = 1 week)
        tokens_per_epoch:                         # Tokens distributed per epoch (unitless amount)
        tokens_for_retroactive_distribution: "0"  # Do not change
        retroactive_distribution_cutoff_time: 0   # Do not change
        distribution_start_time:                  # Start of reward distribution (first claim is 
                                                  # available one epoch period away from start time)
        total_number_of_epochs:                   # Number of epochs to distribute rewards
        initial_epoch_number: 0                   # Do not change
        developer_token_ratio_bps: 1500           # Portion of distribution for developer address 
                                                  # in basis points
        trader_token_ratio_bps: 0                 # Do not change

      incentivized_pools:                         # ZilSwap liquidity pools to receive reward 
                                                  # distributions and their weights

        zil1p5suryq6q647usxczale29cu3336hhp376c627: 2  # ZWAP
        zil14pzuzq6v6pmmmrfjhczywguu0e97djepxt8g3e: 1  # gZIL
```
