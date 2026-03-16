//! Manual borsh deserialization into `serde_json::Value`.
//!
//! Reads bytes according to an IDL type layout and produces JSON values.

use {
    crate::{
        idl::{
            DefinedRef, EnumVariantFields, IdlType, IdlTypeComplex, IdlTypeDef,
            IdlTypeDefBody, StructFields,
        },
        Error,
    },
    serde_json::{Map, Value},
    std::collections::HashMap,
};

/// A cursor over a byte slice with bounds checking.
pub(crate) struct BorshReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BorshReader<'a> {
    pub const fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub const fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.pos + n > self.data.len() {
            return Err(Error::UnexpectedEof {
                needed: n,
                available: self.remaining(),
            });
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        Ok(self.read_bytes(1)?[0])
    }

    fn read_bool(&mut self) -> Result<bool, Error> {
        Ok(self.read_u8()? != 0)
    }

    fn read_u16(&mut self) -> Result<u16, Error> {
        let b = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, Error> {
        let b = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_i32(&mut self) -> Result<i32, Error> {
        let b = self.read_bytes(4)?;
        Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, Error> {
        let b = self.read_bytes(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> Result<i64, Error> {
        let b = self.read_bytes(8)?;
        Ok(i64::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_u128(&mut self) -> Result<u128, Error> {
        let b = self.read_bytes(16)?;
        Ok(u128::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_i128(&mut self) -> Result<i128, Error> {
        let b = self.read_bytes(16)?;
        Ok(i128::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_f32(&mut self) -> Result<f32, Error> {
        let b = self.read_bytes(4)?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_f64(&mut self) -> Result<f64, Error> {
        let b = self.read_bytes(8)?;
        Ok(f64::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_pubkey(&mut self) -> Result<String, Error> {
        let b = self.read_bytes(32)?;
        Ok(bs58::encode(b).into_string())
    }

    fn read_string(&mut self) -> Result<String, Error> {
        let len = self.read_u32()? as usize;
        let b = self.read_bytes(len)?;
        String::from_utf8(b.to_vec()).map_err(|e| Error::InvalidUtf8(e.utf8_error()))
    }

    fn read_borsh_bytes(&mut self) -> Result<Vec<u8>, Error> {
        let len = self.read_u32()? as usize;
        Ok(self.read_bytes(len)?.to_vec())
    }
}

/// Deserialize a value according to `ty`, using `types` to resolve defined references.
pub(crate) fn read_type(
    reader: &mut BorshReader<'_>,
    ty: &IdlType,
    types: &HashMap<String, &IdlTypeDef>,
) -> Result<Value, Error> {
    match ty {
        IdlType::Primitive(name) => read_primitive(reader, name),
        IdlType::Complex(complex) => read_complex(reader, complex, types),
    }
}

fn read_primitive(reader: &mut BorshReader<'_>, name: &str) -> Result<Value, Error> {
    match name {
        "bool" => Ok(Value::Bool(reader.read_bool()?)),
        "u8" => Ok(Value::Number(reader.read_u8()?.into())),
        "u16" => Ok(Value::Number(reader.read_u16()?.into())),
        "u32" => Ok(Value::Number(reader.read_u32()?.into())),
        "i8" => Ok(Value::Number((reader.read_u8()? as i8 as i64).into())),
        "i16" => Ok(Value::Number((reader.read_u16()? as i16 as i64).into())),
        "i32" => Ok(Value::Number(reader.read_i32()?.into())),
        "u64" => {
            let v = reader.read_u64()?;
            // u64 may exceed JSON number precision; emit as string if > 2^53
            if v > (1u64 << 53) {
                Ok(Value::String(v.to_string()))
            } else {
                Ok(Value::Number(v.into()))
            }
        }
        "i64" => Ok(Value::Number(reader.read_i64()?.into())),
        "u128" => Ok(Value::String(reader.read_u128()?.to_string())),
        "i128" => Ok(Value::String(reader.read_i128()?.to_string())),
        "f32" => {
            let v = reader.read_f32()?;
            Ok(serde_json::Number::from_f64(v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        "f64" => {
            let v = reader.read_f64()?;
            Ok(serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        "pubkey" | "publicKey" => Ok(Value::String(reader.read_pubkey()?)),
        "string" => Ok(Value::String(reader.read_string()?)),
        "bytes" => {
            let b = reader.read_borsh_bytes()?;
            Ok(Value::String(bs58::encode(&b).into_string()))
        }
        other => Err(Error::UnknownType(other.to_string())),
    }
}

fn read_complex(
    reader: &mut BorshReader<'_>,
    complex: &IdlTypeComplex,
    types: &HashMap<String, &IdlTypeDef>,
) -> Result<Value, Error> {
    match complex {
        IdlTypeComplex::Vec(inner) => {
            let len = reader.read_u32()? as usize;
            let mut arr = Vec::with_capacity(len);
            for _ in 0..len {
                arr.push(read_type(reader, inner, types)?);
            }
            Ok(Value::Array(arr))
        }
        IdlTypeComplex::Option(inner) => {
            let tag = reader.read_u8()?;
            if tag == 0 {
                Ok(Value::Null)
            } else {
                read_type(reader, inner, types)
            }
        }
        IdlTypeComplex::Array(inner, len) => {
            let mut arr = Vec::with_capacity(*len);
            for _ in 0..*len {
                arr.push(read_type(reader, inner, types)?);
            }
            Ok(Value::Array(arr))
        }
        IdlTypeComplex::Defined(DefinedRef { name }) => read_defined(reader, name, types),
    }
}

fn read_defined(
    reader: &mut BorshReader<'_>,
    name: &str,
    types: &HashMap<String, &IdlTypeDef>,
) -> Result<Value, Error> {
    let def = types
        .get(name)
        .ok_or_else(|| Error::UndefinedType(name.to_string()))?;

    match &def.ty {
        IdlTypeDefBody::Struct { fields } => read_struct_fields(reader, fields, types),
        IdlTypeDefBody::Enum { variants } => {
            let variant_idx = reader.read_u8()? as usize;
            let variant = variants
                .get(variant_idx)
                .ok_or_else(|| Error::InvalidEnumVariant {
                    enum_name: name.to_string(),
                    variant_idx,
                })?;

            match &variant.fields {
                None => Ok(Value::String(variant.name.clone())),
                Some(EnumVariantFields::Named(fields)) => {
                    let mut map = Map::new();
                    map.insert("variant".to_string(), Value::String(variant.name.clone()));
                    for field in fields {
                        let val = read_type(reader, &field.ty, types)?;
                        map.insert(field.name.clone(), val);
                    }
                    Ok(Value::Object(map))
                }
                Some(EnumVariantFields::Tuple(tys)) => {
                    if tys.len() == 1 {
                        let val = read_type(reader, &tys[0], types)?;
                        let mut map = Map::new();
                        map.insert("variant".to_string(), Value::String(variant.name.clone()));
                        map.insert("value".to_string(), val);
                        Ok(Value::Object(map))
                    } else {
                        let mut arr = Vec::with_capacity(tys.len());
                        for ty in tys {
                            arr.push(read_type(reader, ty, types)?);
                        }
                        let mut map = Map::new();
                        map.insert("variant".to_string(), Value::String(variant.name.clone()));
                        map.insert("values".to_string(), Value::Array(arr));
                        Ok(Value::Object(map))
                    }
                }
            }
        }
    }
}

pub(crate) fn read_struct_fields(
    reader: &mut BorshReader<'_>,
    fields: &StructFields,
    types: &HashMap<String, &IdlTypeDef>,
) -> Result<Value, Error> {
    match fields {
        StructFields::Named(named) => {
            let mut map = Map::new();
            for field in named {
                let val = read_type(reader, &field.ty, types)?;
                map.insert(field.name.clone(), val);
            }
            Ok(Value::Object(map))
        }
        StructFields::Tuple(tys) => {
            // Tuple structs with a single field unwrap to the inner value.
            if tys.len() == 1 {
                read_type(reader, &tys[0], types)
            } else {
                let mut arr = Vec::with_capacity(tys.len());
                for ty in tys {
                    arr.push(read_type(reader, ty, types)?);
                }
                Ok(Value::Array(arr))
            }
        }
    }
}
