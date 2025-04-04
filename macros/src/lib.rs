use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use serde::Deserialize;

#[derive(Deserialize)]
struct BuildConfig {
    benches: Vec<String>,
    sensors: Vec<String>,
}

#[proc_macro]
pub fn include_benches(_: TokenStream) -> TokenStream {
    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("build.config.toml").unwrap()).unwrap();
    let mut benches = Vec::new();
    let mut benches_caps = Vec::new();

    for p in config.benches {
        let d_name_caps = format_ident!("{}", p.to_case(Case::Pascal));
        let d_name = format_ident!("{}", p.to_case(Case::Snake));

        benches.push(d_name);
        benches_caps.push(d_name_caps);
    }

    quote! {
        pub fn init_benches() {
            #(
                serde_json::to_string(&#benches::#benches_caps::default()).unwrap();
            )*
        }
    }
    .into()
}

#[proc_macro]
pub fn include_sensors(_: TokenStream) -> TokenStream {
    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("build.config.toml").unwrap()).unwrap();
    let mut sensors = Vec::new();
    let mut sensors_caps = Vec::new();

    for p in config.sensors {
        let p_name_caps = format_ident!("{}", p.to_case(Case::Pascal));
        let p_name = format_ident!("{}", p.to_case(Case::Snake));

        sensors.push(p_name);
        sensors_caps.push(p_name_caps);
    }

    quote! {
        pub static SENSORS: std::sync::OnceLock<std::sync::Mutex<Vec<Box<dyn common::sensor::Sensor>>>> = std::sync::OnceLock::new();

        pub fn init_sensors() {
            SENSORS.set(std::sync::Mutex::new(vec![#(Box::new(#sensors::#sensors_caps::default()),)*])).unwrap();
            #(
                serde_json::to_string(&#sensors::#sensors_caps::default()).unwrap();
            )*
        }
    }
    .into()
}

#[proc_macro]
pub fn plugin_names_str(_: TokenStream) -> TokenStream {
    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("build.config.toml").unwrap()).unwrap();

    let mut names = config
        .benches
        .iter()
        .map(|x| x.to_case(Case::Snake))
        .collect::<Vec<_>>();
    names.extend(config.sensors.iter().map(|x| x.to_case(Case::Snake)));

    quote! {
        [#(#names),*]
    }
    .into()
}
