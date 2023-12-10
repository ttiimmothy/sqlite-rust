use nom::IResult;

pub fn varint(input: &[u8]) -> IResult<&[u8], i64> {
  let mut i = 0;
  let mut value: i64 = (input[i] as i64) & 0x7f;
  while high_bit(input[i]) && i < 8 {
    i += 1;
    value = (value << 7) | ((input[i] as i64) & 0x7f);
  }
  Ok((&input[i + 1..], value))
}

fn high_bit(byte: u8) -> bool {
  (byte & 0xf0) >> 7 == 1
}

#[cfg(test)]
mod tests {
  use super::{high_bit, varint};

  #[test]
  fn test_high_bit() {
    assert!(high_bit(0b10000000));
    assert!(high_bit(0b10111010));
    assert!(!high_bit(0b00000000));
    assert!(!high_bit(0b01111111));
  }

  #[test]
  fn one_byte() {
    let input = &[0x15];
    let (rest, value) = varint(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(value, 0x15);
  }

  #[test]
  fn two_bytes() {
    let input = &[0x87, 0x68];
    let (rest, value) = varint(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(value, 1000);
  }

  #[test]
  fn three_bytes() {
    let input = &[0xc8, 0xf2, 0x19];
    let (rest, value) = varint(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(value, 1194265);
  }

  #[test]
  fn four_bytes() {
    let input = &[0xd1, 0x9a, 0xe2, 0x67];
    let (rest, value) = varint(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(value, 170307943);
  }

  #[test]
  fn nine_bytes() {
    let input = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    let (rest, value) = varint(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(value, 9223372036854775807);
  }

  #[test]
  fn ten_bytes() {
    let input = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xab];
    let (rest, value) = varint(input).unwrap();
    assert!(rest.len() == 1 && rest[0] == 0xab);
    assert_eq!(value, 9223372036854775807);
  }
}
