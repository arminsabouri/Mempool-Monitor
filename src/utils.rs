use anyhow::Result;
use bitcoin::{consensus::Encodable, Amount, FeeRate, Transaction, TxIn};
use bitcoin_hashes::Sha256;
use reqwest::Client as ReqwestClient;
use serde_json::Value;

pub fn get_inputs_hash(inputs: impl IntoIterator<Item = TxIn>) -> Result<String> {
    let mut engine = Sha256::engine();
    for i in inputs {
        let mut writer = vec![];
        i.consensus_encode(&mut writer)
            .expect("encoding doesn't error");
        std::io::copy(&mut writer.as_slice(), &mut engine).expect("engine writes don't error");
    }

    let hash = Sha256::from_engine(engine);
    let hash_bytes = hash.as_byte_array().to_vec();
    Ok(hex::encode(hash_bytes))
}

/// Compute the fee rate of a transaction
pub fn compute_fee_rate(tx: &Transaction, absolute_fee: Amount) -> Result<FeeRate> {
    if tx.is_coinbase() {
        return Ok(FeeRate::ZERO);
    }
    let weight = tx.weight();
    let fee_rate = FeeRate::from_sat_per_vb(absolute_fee.to_sat() / weight.to_vbytes_ceil())
        .ok_or(anyhow::anyhow!("Fee rate is 0"))?;
    Ok(fee_rate)
}

pub async fn get_hash_rate_distribution() -> Result<String> {
    let reqwest_client = ReqwestClient::new();
    let response = reqwest_client
        .get("https://mempool.space/api/v1/mining/hashrate/pools/1m")
        .send()
        .await?
        .text()
        .await?;

    let json: Value = serde_json::from_str(&response)?;
    Ok(json.to_string())
}
