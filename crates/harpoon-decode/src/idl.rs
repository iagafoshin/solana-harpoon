//! Anchor IDL JSON schema (deserialization only).
//!
//! Supports both the modern Anchor v0.30+ format (explicit discriminators in the IDL)
//! and older formats (sha256-derived discriminators).

use {serde::Deserialize, std::collections::HashMap};

/// Root of an Anchor IDL JSON file.
#[derive(Debug, Deserialize)]
pub struct Idl {
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub instructions: Vec<IdlInstruction>,
    #[serde(default)]
    pub events: Vec<IdlEvent>,
    #[serde(default)]
    pub accounts: Vec<IdlAccount>,
    #[serde(default)]
    pub types: Vec<IdlTypeDef>,
}

#[derive(Debug, Deserialize)]
pub struct IdlInstruction {
    pub name: String,
    #[serde(default)]
    pub discriminator: Option<Vec<u8>>,
    #[serde(default)]
    pub args: Vec<IdlField>,
}

#[derive(Debug, Deserialize)]
pub struct IdlEvent {
    pub name: String,
    #[serde(default)]
    pub discriminator: Option<Vec<u8>>,
}

#[derive(Debug, Deserialize)]
pub struct IdlAccount {
    pub name: String,
    #[serde(default)]
    pub discriminator: Option<Vec<u8>>,
}

#[derive(Debug, Deserialize)]
pub struct IdlTypeDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: IdlTypeDefBody,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
pub enum IdlTypeDefBody {
    #[serde(rename = "struct")]
    Struct { fields: StructFields },
    #[serde(rename = "enum")]
    Enum { variants: Vec<IdlEnumVariant> },
}

/// Struct fields can be either named (objects) or tuple (bare types).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum StructFields {
    Named(Vec<IdlField>),
    Tuple(Vec<IdlType>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdlField {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: IdlType,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdlEnumVariant {
    pub name: String,
    #[serde(default)]
    pub fields: Option<EnumVariantFields>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum EnumVariantFields {
    Named(Vec<IdlField>),
    Tuple(Vec<IdlType>),
}

/// Represents a Borsh/Anchor type.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum IdlType {
    Primitive(String),
    Complex(IdlTypeComplex),
}

#[derive(Debug, Clone, Deserialize)]
pub enum IdlTypeComplex {
    #[serde(rename = "vec")]
    Vec(Box<IdlType>),
    #[serde(rename = "option")]
    Option(Box<IdlType>),
    #[serde(rename = "array")]
    Array(Box<IdlType>, usize),
    #[serde(rename = "defined")]
    Defined(DefinedRef),
}

#[derive(Debug, Clone, Deserialize)]
pub struct DefinedRef {
    pub name: String,
}

impl Idl {
    /// Build a lookup map of type definitions by name.
    pub fn type_map(&self) -> HashMap<String, &IdlTypeDef> {
        let mut map = HashMap::new();
        for td in &self.types {
            map.insert(td.name.clone(), td);
        }
        map
    }
}
