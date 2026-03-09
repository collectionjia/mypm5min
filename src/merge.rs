
/// 尝试 Redeem 指定 condition_id 的所有非零持仓（Winning Side）。
/// 如果市场未决议，或者没有任何持仓获胜，交易可能会 Revert。
///
/// - `condition_id`: 市场的 condition ID
/// - `proxy`: Proxy 地址
/// - `private_key`: EOA 私钥
/// - `rpc_url`: Polygon RPC
pub async fn redeem_max(
    condition_id: B256,
    proxy: Address,
    private_key: &str,
    rpc_url: Option<&str>,
) -> Result<String> {
    let rpc = rpc_url.unwrap_or(RPC_URL_DEFAULT);
    let chain = POLYGON;
    let signer = LocalSigner::from_str(private_key)?.with_chain_id(Some(chain));
    let wallet = signer.address();

    let provider = ProviderBuilder::new().wallet(signer.clone()).connect(rpc).await?;
    let client = Client::new(provider.clone(), chain)?;
    let config = contract_config(chain, false).ok_or_else(|| anyhow::anyhow!("不支持的 chain_id: {}", chain))?;
    let prov_read = ProviderBuilder::new().connect(rpc).await?;
    let erc1155 = IERC1155Balance::new(config.conditional_tokens, prov_read);
    let ctf = config.conditional_tokens;

    let req_col_yes = CollectionIdRequest::builder().parent_collection_id(B256::ZERO).condition_id(condition_id).index_set(U256::from(1)).build();
    let req_col_no = CollectionIdRequest::builder().parent_collection_id(B256::ZERO).condition_id(condition_id).index_set(U256::from(2)).build();
    let col_yes = client.collection_id(&req_col_yes).await?;
    let col_no = client.collection_id(&req_col_no).await?;

    let req_pos_yes = PositionIdRequest::builder().collateral_token(USDC_POLYGON).collection_id(col_yes.collection_id).build();
    let req_pos_no = PositionIdRequest::builder().collateral_token(USDC_POLYGON).collection_id(col_no.collection_id).build();
    let pos_yes = client.position_id(&req_pos_yes).await?;
    let pos_no = client.position_id(&req_pos_no).await?;

    let b_yes: U256 = erc1155.balanceOf(proxy, pos_yes.position_id).call().await.unwrap_or(U256::ZERO);
    let b_no: U256 = erc1155.balanceOf(proxy, pos_no.position_id).call().await.unwrap_or(U256::ZERO);

    let mut index_sets = Vec::new();
    if b_yes > U256::ZERO {
        index_sets.push(U256::from(1));
    }
    if b_no > U256::ZERO {
        index_sets.push(U256::from(2));
    }

    if index_sets.is_empty() {
        anyhow::bail!("无持仓可 Redeem：YES={} NO={}", b_yes, b_no);
    }
    info!("🔄 尝试 Redeem 持仓: YES={} NO={} (IndexSets: {:?})", b_yes, b_no, index_sets);

    // 构建 calldata
    let redeem_calldata = encode_redeem_calldata(USDC_POLYGON, B256::ZERO, condition_id, index_sets);
    let code = provider.get_code_at(proxy).await.unwrap_or_default();

    if code.len() < 150 {
        // Relayer Logic (copy from merge_max)
        let builder_key = env::var("POLY_BUILDER_API_KEY").ok();
        let builder_secret = env::var("POLY_BUILDER_SECRET").ok();
        let builder_passphrase = env::var("POLY_BUILDER_PASSPHRASE").ok();
        let relayer_url = env::var("RELAYER_URL").unwrap_or_else(|_| RELAYER_URL_DEFAULT.to_string());
        match (builder_key.as_deref(), builder_secret.as_deref(), builder_passphrase.as_deref()) {
            (Some(k), Some(s), Some(p)) => {
                let out = relayer_execute_merge(&redeem_calldata, ctf, proxy, &signer, k, s, p, &relayer_url).await?;
                info!("✅ Relayer 已提交 Redeem tx: {}", out);
                return Ok(out);
            }
            _ => anyhow::bail!(
                "Magic/Email Redeem 需配置 POLY_BUILDER_API_KEY 等。",
            ),
        }
    }

    // Safe Logic
    let safe = IGnosisSafe::new(proxy, provider);
    let nonce: U256 = safe.nonce().call().await.map_err(|e| anyhow::anyhow!("读取 Safe nonce 失败: {}", e))?;

    let tx_hash_data = safe
        .encodeTransactionData(ctf, U256::ZERO, redeem_calldata.clone().into(), 0u8, U256::ZERO, U256::ZERO, U256::ZERO, Address::ZERO, Address::ZERO, nonce)
        .call().await.map_err(|e| anyhow::anyhow!("Safe.encodeTransactionData 失败: {}", e))?.0;

    let tx_hash = keccak256(tx_hash_data.as_ref());
    let sig = signer.sign_hash(&tx_hash).await.map_err(|e| anyhow::anyhow!("签名失败: {}", e))?;
    let mut sig_bytes = sig.as_bytes().to_vec();
    if sig_bytes.len() == 65 && (sig_bytes[64] == 0 || sig_bytes[64] == 1) {
        sig_bytes[64] += 27;
    }

    let pending = safe
        .execTransaction(ctf, U256::ZERO, redeem_calldata.into(), 0u8, U256::ZERO, U256::ZERO, U256::ZERO, Address::ZERO, Address::ZERO, sig_bytes.into())
        .send().await.map_err(|e| anyhow::anyhow!("Safe.execTransaction (Redeem) 失败: {}", e))?;

    let tx_hash_out = *pending.tx_hash();
    info!("✅ Redeem 交易已提交（Safe）tx: {:#x}", tx_hash_out);
    Ok(format!("{:#x}", tx_hash_out))
}
