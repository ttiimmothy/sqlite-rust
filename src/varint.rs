use anyhow::{anyhow, Result};

pub fn read_varint(bytes: &[u8]) -> Result<(u64, &[u8], usize)> {
  let mut result = 0;
  let shift = 7;
  let mut bytes_read = 0;
  let mut bs = bytes.iter().copied();
  loop {
    let byte = bs.next().ok_or_else(|| anyhow!("Unexpected end of bytes"))?;
    bytes_read += 1;
    if bytes_read == 9 {
      result = (result << shift) | u64::from(byte);
      break;
    }
    result = (result << shift) | u64::from(byte & 0b0111_1111);
    if byte & 0b1000_0000 == 0 {
      break;
    }
  }
  Ok((result, &bytes[bytes_read..], bytes_read))
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn test_one_byte() -> Result<()> {
    let varint = vec![0b0000_0001, 0b1111_1111];
    let (n, _, _) = read_varint(&varint)?;
    assert_eq!(n, 1);
    Ok(())
  }

  #[test]
  fn test_two_byte() -> Result<()> {
    let varint = vec![0b1000_0001, 0b0111_1111, 0];
    let (n, _, _) = read_varint(&varint)?;
    assert_eq!(n, 255);
    Ok(())
  }

  #[test]
  fn test_max_varint() -> Result<()> {
    let varint = vec![
      0b1000_0001,
      0b1000_0001,
      0b1000_0001,
      0b1000_0001,
      0b1000_0001,
      0b1000_0001,
      0b1000_0001,
      0b1000_0001,
      0b0000_0001,
    ];
    let (n, _, _) = read_varint(&varint)?;
    assert_eq!(n, (1u64 << 56)
      | (1u64 << 49)
      | (1u64 << 42)
      | (1u64 << 35)
      | (1u64 << 28)
      | (1u64 << 21)
      | (1u64 << 14)
      | (1u64 << 7)
      | 1u64
    );
    Ok(())
  }

  #[test]
  fn test_incomplete_varint() {
    let varint = vec![0b1000_0000];
    assert!(read_varint(&varint).is_err());
  }

  #[test]
  fn test_empty_input() {
    let varint = vec![];
    assert!(read_varint(&varint).is_err());
  }

  #[test]
  fn test_multiple_varints() -> Result<()> {
    let varints = vec![
      0b0000_0010,
      0b1000_0010,
      0b0000_0001,
      0b1000_0001,
      0b0000_0001,
    ];
    let (n1, rest, _) = read_varint(&varints)?;
    assert_eq!(n1, 2);
    let (n2, rest, _) = read_varint(rest)?;
    assert_eq!(n2, 257);
    let (n3, _, _) = read_varint(rest)?;
    assert_eq!(n3, 129);
    Ok(())
  }

  #[test]
  fn test_varint_with_additional_data() -> Result<()> {
    let varint_with_data = vec![0b0000_0011, 0xff, 0xee];
    let (n, rest, _) = read_varint(&varint_with_data)?;
    assert_eq!(n, 3);
    assert_eq!(rest, &[0xff, 0xee]);
    Ok(())
  }

  #[test]
  fn test_largest_single_byte_varint() -> Result<()> {
    let varint = vec![0b0111_1111];
    let (n, _, _) = read_varint(&varint)?;
    assert_eq!(n, 127);
    Ok(())
  }
}