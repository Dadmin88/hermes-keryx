use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::error::{validate_identifier, ValidationError};

macro_rules! string_id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> Result<Self, ValidationError> {
                validate_identifier(stringify!($name), value).map(Self)
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = ValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

string_id_type!(AgentId);
string_id_type!(CapabilityId);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Skill {
    id: CapabilityId,
}

impl Skill {
    pub fn new(id: CapabilityId) -> Self {
        Self { id }
    }

    #[must_use]
    pub fn id(&self) -> &CapabilityId {
        &self.id
    }
}

impl From<CapabilityId> for Skill {
    fn from(id: CapabilityId) -> Self {
        Self::new(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCard {
    id: AgentId,
    name: String,
    description: String,
    skills: Vec<Skill>,
}

impl AgentCard {
    pub fn new(
        id: AgentId,
        name: impl Into<String>,
        description: impl Into<String>,
    ) -> Result<Self, ValidationError> {
        let name = validate_identifier("AgentCardName", name.into())?;
        let description = description.into().trim().to_owned();

        Ok(Self {
            id,
            name,
            description,
            skills: Vec::new(),
        })
    }

    #[must_use]
    pub fn id(&self) -> &AgentId {
        &self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub fn skills(&self) -> &[Skill] {
        &self.skills
    }

    pub fn add_skill(&mut self, skill: impl Into<Skill>) -> bool {
        let skill = skill.into();
        if self.skills.iter().any(|existing| existing.id == skill.id) {
            return false;
        }
        self.skills.push(skill);
        true
    }

    #[must_use]
    pub fn has_skill(&self, capability_id: &CapabilityId) -> bool {
        self.skills.iter().any(|skill| &skill.id == capability_id)
    }

    #[must_use]
    pub fn skill(&self, capability_id: &CapabilityId) -> Option<&Skill> {
        self.skills.iter().find(|skill| &skill.id == capability_id)
    }
}
