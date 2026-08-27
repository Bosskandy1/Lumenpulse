#![cfg(test)]

use crate::LiquidityPoolError;
use crate::{LiquidityPoolContract, LiquidityPoolContractClient};
use soroban_sdk::{
    testutils::Address as _,
    token::{StellarAssetClient, TokenClient},
    Address, Env,
};

fn create_token_contract<'a>(
    env: &Env,
    admin: &Address,
) -> (TokenClient<'a>, StellarAssetClient<'a>) {
    let contract_address = env.register_stellar_asset_contract_v2(admin.clone());
    (
        TokenClient::new(env, &contract_address.address()),
        StellarAssetClient::new(env, &contract_address.address()),
    )
}

fn setup_test<'a>(
    env: &Env,
) -> (
    LiquidityPoolContractClient<'a>,
    Address,                // admin
    Address,                // token_0_addr
    Address,                // token_1_addr
    TokenClient<'a>,        // token_0_client
    TokenClient<'a>,        // token_1_client
    StellarAssetClient<'a>, // token_0_admin
    StellarAssetClient<'a>, // token_1_admin
) {
    let admin = Address::generate(env);
    let token_admin = Address::generate(env);

    let (token_0, token_0_admin) = create_token_contract(env, &token_admin);
    let (token_1, token_1_admin) = create_token_contract(env, &token_admin);

    let contract_id = env.register(LiquidityPoolContract, ());
    let client = LiquidityPoolContractClient::new(env, &contract_id);

    (
        client,
        admin,
        token_0.address.clone(),
        token_1.address.clone(),
        token_0,
        token_1,
        token_0_admin,
        token_1_admin,
    )
}

#[test]
fn test_initialize_and_auth() {
    let env = Env::default();
    env.mock_all_auths();

    let (client, admin, token_0, token_1, _, _, _, _) = setup_test(&env);

    // Initializing contract
    client.initialize(&admin, &token_0, &token_1);

    // Trying to initialize again should fail
    let res = client.try_initialize(&admin, &token_0, &token_1);
    assert_eq!(
        res.err().unwrap().unwrap(),
        LiquidityPoolError::AlreadyInitialized
    );
}

#[test]
fn test_initial_liquidity_provision() {
    let env = Env::default();
    env.mock_all_auths();

    let (
        client,
        admin,
        token_0_addr,
        token_1_addr,
        _token_0,
        _token_1,
        token_0_admin,
        token_1_admin,
    ) = setup_test(&env);
    client.initialize(&admin, &token_0_addr, &token_1_addr);

    let alice = Address::generate(&env);
    token_0_admin.mint(&alice, &10_000);
    token_1_admin.mint(&alice, &40_000);

    // Alice deposits 10,000 token_0 and 40,000 token_1
    let lp_received = client.add_liquidity(&alice, &10_000, &40_000, &20_000);
    assert_eq!(lp_received, 20_000);

    // Verify reserves and user balance
    let (reserve_0, reserve_1) = client.get_reserves();
    assert_eq!(reserve_0, 10_000);
    assert_eq!(reserve_1, 40_000);
    assert_eq!(client.lp_balance(&alice), 20_000);
}

#[test]
fn test_subsequent_liquidity_provision() {
    let env = Env::default();
    env.mock_all_auths();

    let (
        client,
        admin,
        token_0_addr,
        token_1_addr,
        _token_0,
        _token_1,
        token_0_admin,
        token_1_admin,
    ) = setup_test(&env);
    client.initialize(&admin, &token_0_addr, &token_1_addr);

    let alice = Address::generate(&env);
    let bob = Address::generate(&env);

    token_0_admin.mint(&alice, &10_000);
    token_1_admin.mint(&alice, &40_000);
    token_0_admin.mint(&bob, &5_000);
    token_1_admin.mint(&bob, &20_000);

    // Alice provides initial liquidity
    client.add_liquidity(&alice, &10_000, &40_000, &20_000);

    // Bob provides subsequent liquidity
    // New LP = min(amount_0 * lp_supply / (reserve_0 + 1), amount_1 * lp_supply / (reserve_1 + 1))
    // lp_0 = 5000 * 20000 / 10001 = 9999
    // lp_1 = 20000 * 20000 / 40001 = 9999
    let bob_lp = client.add_liquidity(&bob, &5_000, &20_000, &9_999);
    assert_eq!(bob_lp, 9_999);

    let (r0, r1) = client.get_reserves();
    assert_eq!(r0, 15_000);
    assert_eq!(r1, 60_000);
    assert_eq!(client.lp_balance(&bob), 9_999);
}

#[test]
fn test_redemption() {
    let env = Env::default();
    env.mock_all_auths();

    let (
        client,
        admin,
        token_0_addr,
        token_1_addr,
        _token_0,
        _token_1,
        token_0_admin,
        token_1_admin,
    ) = setup_test(&env);
    client.initialize(&admin, &token_0_addr, &token_1_addr);

    let alice = Address::generate(&env);
    let bob = Address::generate(&env);

    token_0_admin.mint(&alice, &10_000);
    token_1_admin.mint(&alice, &40_000);
    token_0_admin.mint(&bob, &5_000);
    token_1_admin.mint(&bob, &20_000);

    // Provision
    client.add_liquidity(&alice, &10_000, &40_000, &20_000);
    client.add_liquidity(&bob, &5_000, &20_000, &9_999);

    // Alice partial redemption: removes 10,000 LP tokens (half of her stake)
    // out_0 = 10000 * 15000 / 29999 = 5000
    // out_1 = 10000 * 60000 / 29999 = 20000
    let (alice_0, alice_1) = client.remove_liquidity(&alice, &10_000, &5_000, &20_000);
    assert_eq!(alice_0, 5_000);
    assert_eq!(alice_1, 20_000);
    assert_eq!(client.lp_balance(&alice), 10_000);

    // Bob full redemption: removes 9,999 LP tokens
    // remaining lp_supply = 19999
    // out_0 = 9999 * 10000 / 19999 = 4999
    // out_1 = 9999 * 40000 / 19999 = 19998
    let (bob_0, bob_1) = client.remove_liquidity(&bob, &9_999, &4_999, &19_998);
    assert_eq!(bob_0, 4_999);
    assert_eq!(bob_1, 19_998);
    assert_eq!(client.lp_balance(&bob), 0);
}

#[test]
fn test_proportional_withdrawal_unequal_stakes() {
    let env = Env::default();
    env.mock_all_auths();

    let (
        client,
        admin,
        token_0_addr,
        token_1_addr,
        _token_0,
        _token_1,
        token_0_admin,
        token_1_admin,
    ) = setup_test(&env);
    client.initialize(&admin, &token_0_addr, &token_1_addr);

    let alice = Address::generate(&env);
    let bob = Address::generate(&env);
    let charlie = Address::generate(&env);

    // Initial mints
    token_0_admin.mint(&alice, &10_000);
    token_1_admin.mint(&alice, &10_000);
    token_0_admin.mint(&bob, &6_000);
    token_1_admin.mint(&bob, &6_000);
    token_0_admin.mint(&charlie, &4_000);
    token_1_admin.mint(&charlie, &4_000);

    // Alice deposits (50% target stake)
    let alice_lp = client.add_liquidity(&alice, &10_000, &10_000, &10_000);
    assert_eq!(alice_lp, 10_000);

    // Bob deposits (30% target stake)
    // lp_0 = 6000 * 10000 / 10001 = 5999
    let bob_lp = client.add_liquidity(&bob, &6_000, &6_000, &5_999);
    assert_eq!(bob_lp, 5_999);

    // Charlie deposits (20% target stake)
    // lp_0 = 4000 * 15999 / 16001 = 3999
    let charlie_lp = client.add_liquidity(&charlie, &4_000, &4_000, &3_999);
    assert_eq!(charlie_lp, 3_999);

    let lp_supply =
        client.lp_balance(&alice) + client.lp_balance(&bob) + client.lp_balance(&charlie);
    assert_eq!(lp_supply, 19_998);

    // Redeem Bob's stake (30% of reserves)
    // Bob receives: out_0 = 5999 * 20000 / 19998 = 5999
    let (bob_0, bob_1) = client.remove_liquidity(&bob, &5_999, &5_999, &5_999);
    assert_eq!(bob_0, 5_999);
    assert_eq!(bob_1, 5_999);

    // Redeem Charlie's stake (20% of reserves)
    // Charlie receives: out_0 = 3999 * 14001 / 13999 = 3999 due to truncation
    let (charlie_0, charlie_1) = client.remove_liquidity(&charlie, &3_999, &3_999, &3_999);
    assert_eq!(charlie_0, 3_999);
    assert_eq!(charlie_1, 3_999);
}

#[test]
fn test_first_depositor_share_inflation_mitigation() {
    let env = Env::default();
    env.mock_all_auths();

    let (client, admin, token_0_addr, token_1_addr, token_0, token_1, token_0_admin, token_1_admin) =
        setup_test(&env);
    client.initialize(&admin, &token_0_addr, &token_1_addr);

    let attacker = Address::generate(&env);
    let victim = Address::generate(&env);

    // Attacker gets minimal tokens
    token_0_admin.mint(&attacker, &1_000_000);
    token_1_admin.mint(&attacker, &1_000_000);

    // Victim gets tokens
    token_0_admin.mint(&victim, &100_000);
    token_1_admin.mint(&victim, &100_000);

    // 1. Attacker makes tiny initial deposit to get 1 LP share
    client.add_liquidity(&attacker, &1, &1, &1);
    assert_eq!(client.lp_balance(&attacker), 1);

    // 2. Attacker simulates "donation" by transferring tokens directly to contract address
    // (skipping the add_liquidity function).
    token_0.transfer(&attacker, &client.address, &100_000);
    token_1.transfer(&attacker, &client.address, &100_000);

    // Verify token balance of contract is indeed high, but pool reserve is still 1
    assert_eq!(token_0.balance(&client.address), 100_001);
    let (r0, _) = client.get_reserves();
    assert_eq!(r0, 1);

    // 3. Victim deposits 100,000 of token_0 and 100,000 of token_1
    // lp_0 = (100000 * 1) / (1 + 1) = 50000
    // Because the contract uses internal tracked reserves (+1 offset) rather than raw token balance of contract,
    // the victim receives 50,000 LP tokens.
    // If it used actual token balances, victim would get: 100000 * 1 / 100001 = 0 LP shares.
    let victim_lp = client.add_liquidity(&victim, &100_000, &100_000, &50_000);
    assert_eq!(victim_lp, 50_000);
    assert_eq!(client.lp_balance(&victim), 50_000);
}

#[test]
fn test_checked_arithmetic_overflow_underflow() {
    let env = Env::default();
    env.mock_all_auths();

    let (client, admin, token_0_addr, token_1_addr, _, _, token_0_admin, token_1_admin) =
        setup_test(&env);
    client.initialize(&admin, &token_0_addr, &token_1_addr);

    let alice = Address::generate(&env);
    token_0_admin.mint(&alice, &100_000);
    token_1_admin.mint(&alice, &100_000);

    // 1. Call add_liquidity with negative amount_0
    let res = client.try_add_liquidity(&alice, &-1000, &1000, &100);
    assert_eq!(
        res.err().unwrap().unwrap(),
        LiquidityPoolError::InvalidAmount
    );

    // 2. Call add_liquidity with zero amount_1
    let res = client.try_add_liquidity(&alice, &1000, &0, &100);
    assert_eq!(
        res.err().unwrap().unwrap(),
        LiquidityPoolError::InvalidAmount
    );

    // 3. Call remove_liquidity with zero LP amount
    let res = client.try_remove_liquidity(&alice, &0, &100, &100);
    assert_eq!(
        res.err().unwrap().unwrap(),
        LiquidityPoolError::InvalidAmount
    );

    // 4. Call remove_liquidity with negative LP amount
    let res = client.try_remove_liquidity(&alice, &-500, &100, &100);
    assert_eq!(
        res.err().unwrap().unwrap(),
        LiquidityPoolError::InvalidAmount
    );

    // 5. Try to remove more LP tokens than balance
    let res = client.try_remove_liquidity(&alice, &1000, &100, &100);
    assert_eq!(
        res.err().unwrap().unwrap(),
        LiquidityPoolError::InsufficientBalance
    );
}
