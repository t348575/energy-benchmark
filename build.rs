use eyre::{Context, ContextCompat, Result};
use regex::Regex;
use std::fs::{read_to_string, write};
use std::path::Path;
use toml_edit::{DocumentMut, Item, Value, table, value};

const ITEMS: &[&str] = &["benches", "plots", "sensors"];

fn process_components(
    config: &toml::Value,
    workspace: &mut DocumentMut,
    component_type: &str,
) -> Result<()> {
    let default_member = format!("{}/default-{}", component_type, component_type);
    let members = workspace["workspace"]["members"]
        .as_array_mut()
        .context("Reading workspace members")?;

    members.retain(|m| {
        let member = m.as_str().unwrap();
        !member.starts_with(&format!("{}/", component_type)) || member == default_member
    });

    let cargo_path = format!("{}/Cargo.toml", default_member);
    println!("cargo:rerun-if-changed={}", cargo_path);
    let mut cargo_doc = read_to_string(&cargo_path)?.parse::<DocumentMut>()?;
    let deps = cargo_doc["dependencies"]
        .as_table_mut()
        .context("Reading dependencies")?;

    let pattern = Regex::new(&format!(r"^\.\./{}/", component_type))?;
    deps.retain(|_, dep_item| {
        if let Some(dep) = dep_item.as_inline_table() {
            if let Some(path) = dep.get("path").and_then(|p| p.as_str()) {
                if path.starts_with("../") && path.ne("../../macros") {
                    return false;
                }
            }
        }
        true
    });
    deps.retain(|_, v| {
        !v.get("path")
            .and_then(|p| p.as_str())
            .is_some_and(|p| pattern.is_match(p))
    });

    if let Some(items) = config.get(component_type).and_then(|v| v.as_array()) {
        for item in items {
            let item_str = item.as_str().unwrap();
            let mut dep_table = table();
            dep_table["path"] = value(format!("../{}", item_str));

            if let Some(overrides) = config.get(item_str).and_then(|v| v.as_table()) {
                for (k, v) in overrides.iter() {
                    match v {
                        toml::Value::Array(arr) => {
                            let mut edit_arr = toml_edit::Array::default();
                            for elem in arr.iter() {
                                if let toml::Value::String(s) = elem {
                                    edit_arr.push(s.as_str());
                                }
                            }
                            dep_table[k] = Item::Value(Value::Array(edit_arr));
                        }
                        toml::Value::Boolean(b) => {
                            dep_table[k] = value(*b);
                        }
                        toml::Value::String(s) => {
                            dep_table[k] = value(s.clone());
                        }
                        toml::Value::Integer(i) => {
                            dep_table[k] = value(*i);
                        }
                        _ => unimplemented!(),
                    }
                }
            }

            deps.insert(
                item_str,
                Item::Value(Value::InlineTable(
                    dep_table.into_table().unwrap().into_inline_table(),
                )),
            );
        }
    }

    write(cargo_path, cargo_doc.to_string())?;
    Ok(())
}

fn main() -> Result<()> {
    let config_content = read_to_string("setup.toml").context("Reading setup.toml")?;
    let config = config_content.parse::<toml::Value>()?;

    let workspace_path = Path::new("Cargo.toml");
    let mut workspace_doc = read_to_string(workspace_path)?
        .parse::<DocumentMut>()
        .context("Reading Cargo.toml")?;

    for component in ITEMS {
        process_components(&config, &mut workspace_doc, component)
            .context(format!("Processing {}", component))?;
    }

    let app_path = Path::new("app/Cargo.toml");
    let mut app_doc = read_to_string(app_path)?.parse::<DocumentMut>()?;
    let app_deps = app_doc["dependencies"]
        .as_table_mut()
        .context("Reading app/Cargo.toml")?;

    for component in ITEMS {
        let key = format!("default-{}", component);
        let mut dep_table = table();
        dep_table["path"] = value(format!("../{}/default-{}", component, component));

        app_deps.insert(
            &key,
            Item::Value(Value::InlineTable(
                dep_table.into_table().unwrap().into_inline_table(),
            )),
        );
    }

    write(workspace_path, workspace_doc.to_string())?;
    write(app_path, app_doc.to_string())?;

    println!("cargo:rerun-if-changed=setup.toml");

    Ok(())
}
