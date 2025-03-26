use bitcoin::{consensus::Encodable, Transaction, TxIn};
use anyhow::Result;
use bitcoin_hashes::Sha256;


// Prune tx witness in place
pub fn prune_large_witnesses(tx: &mut Transaction) {
    tx.input.iter_mut().for_each(|input| {
        input.witness.clear();
    });
}

pub fn get_inputs_hash(inputs: impl IntoIterator<Item = TxIn>) -> Result<Vec<u8>> {
    let mut engine = Sha256::engine();
    for i in inputs {
        let mut writer = vec![];
        i.consensus_encode(&mut writer)
            .expect("encoding doesn't error");
        std::io::copy(&mut writer.as_slice(), &mut engine).expect("engine writes don't error");
    }

    let hash = Sha256::from_engine(engine);
    let hash_bytes = hash.as_byte_array().to_vec();
    Ok(hash_bytes)
}
