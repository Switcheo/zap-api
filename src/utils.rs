use bigdecimal::{BigDecimal, Zero};
use num_bigint::BigInt;

pub fn round_down(bd: BigDecimal, round_digits: i64) -> BigDecimal {
  let (bigint, decimal_part_digits) = bd.as_bigint_and_exponent();
  let need_to_round_digits = decimal_part_digits - round_digits;
  if round_digits >= 0 && need_to_round_digits <= 0 {
      return bd.clone();
  }
  let mut number = bigint.clone();
  if number < BigInt::zero() {
      number = -number;
  }
  for _ in 0..(need_to_round_digits - 1) {
      number /= 10;
  }
  bd.with_scale(round_digits)
}

// pub fn to_hex(bytes: Vec<u8>) -> String {
//   let ss: Vec<String> = bytes.iter().map(|b| format!("{:02X}", b)).collect();
//   ss.concat()
// }

// pub fn to_str<T>(proof: Proof<T>) -> String {
//   let mut lemma = Box::new(proof.lemma);
//   loop {
//     if let Some(sibling) = lemma.sibling_hash {
//       // xxx: hack to get hash because enum is unexported(?)
//       let debug_str = format!("{:?}", sibling);
//       let str_arr = debug_str
//         .replace("Left", "").replace("Right", "").replace(",", "")
//         .replace("[", "").replace("]", "")
//         .replace("(", "").replace(")", "");
//       let arr: Vec<u8> = str_arr.split(' ').into_iter().map(|x| u8::from_str_radix(x, 10).unwrap()).collect();
//       println!("Sibling hash: {}", encode(arr));
//     }
//     match lemma.sub_lemma {
//       None => {
//         println!("Node hash: {}", encode(lemma.node_hash.to_vec()));
//         break
//       }
//       Some(l) => lemma = l
//     }
//   }
//   String::from("")
// }
