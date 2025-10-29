use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use serde::Deserialize;
use syn::{Expr, Token, parse_macro_input, punctuated::Punctuated};

#[derive(Deserialize)]
struct BuildConfig {
    benches: Vec<String>,
    sensors: Vec<String>,
    plots: Vec<String>,
}

#[proc_macro]
pub fn include_benches(_: TokenStream) -> TokenStream {
    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("setup.toml").unwrap()).unwrap();
    let mut benches = Vec::new();
    let mut benches_caps = Vec::new();

    for p in config.benches {
        let d_name_caps = format_ident!("{}", p.to_case(Case::Pascal));
        let d_name = format_ident!("{}", p.replace("-", "_"));

        benches.push(d_name);
        benches_caps.push(d_name_caps);
    }

    quote! {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        pub enum BenchKind {
            #(#benches_caps),*
        }

        pub fn init_benches() {
            #(
                serde_json::to_string(&#benches::#benches_caps::default()).unwrap();
            )*
        }

        impl BenchKind {
            pub fn name(&self) -> &'static str {
                use common::bench::Bench;
                match *self {
                    #(
                        BenchKind::#benches_caps => #benches::#benches_caps::default().name(),
                    )*
                }
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn if_sensor(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input with Punctuated::<Expr, Token![,]>::parse_terminated);

    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("setup.toml").unwrap()).unwrap();

    let mut iter = args.into_iter();
    let sensor_name_expr = iter.next().unwrap();
    let exists_value = iter.next().unwrap();
    let not_exists_value = iter.next().unwrap();

    let sensor_name_literal = if let Expr::Lit(expr_lit) = &sensor_name_expr
        && let syn::Lit::Str(lit_str) = &expr_lit.lit
    {
        lit_str.value()
    } else {
        return syn::Error::new_spanned(
            &sensor_name_expr,
            "first argument must be a string literal",
        )
        .to_compile_error()
        .into();
    };

    if config
        .sensors
        .iter()
        .any(|s| s.to_case(Case::Pascal) == sensor_name_literal)
    {
        quote! { #exists_value }
    } else {
        quote! { #not_exists_value }
    }
    .into()
}

#[proc_macro]
pub fn sensor_kind(_: TokenStream) -> TokenStream {
    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("setup.toml").unwrap()).unwrap();

    let sensors_caps = config
        .sensors
        .iter()
        .map(|p| format_ident!("{}", p.to_case(Case::Pascal)))
        .collect::<Vec<_>>();
    let sensors_str = config
        .sensors
        .iter()
        .map(|p| p.to_case(Case::Pascal))
        .collect::<Vec<_>>();

    quote! {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        pub enum SensorKind {
            #(#sensors_caps),*
        }

        impl SensorKind {
            pub fn get(item: &str) -> Option<SensorKind> {
                match item {
                    #(
                        #sensors_str => Some(SensorKind::#sensors_caps),
                    )*
                    _ => None,
                }
            }
        }

        impl core::fmt::Display for SensorKind {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                match self {
                    #(
                        SensorKind::#sensors_caps => write!(f, "{}", #sensors_str),
                    )*
                }
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn include_sensors(_: TokenStream) -> TokenStream {
    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("setup.toml").unwrap()).unwrap();
    let mut sensors = Vec::new();
    let mut sensors_caps = Vec::new();
    let mut sensor_config = Vec::new();

    for p in config.sensors {
        let p_name_caps = format_ident!("{}", p.to_case(Case::Pascal));
        let p_name_config = format_ident!("{}Config", p.to_case(Case::Pascal));
        let p_name = format_ident!("{}", p.replace("-", "_"));

        sensors.push(p_name);
        sensors_caps.push(p_name_caps);
        sensor_config.push(p_name_config);
    }

    quote! {
        pub static SENSORS: std::sync::OnceLock<Vec<Box<dyn common::sensor::Sensor>>> = std::sync::OnceLock::new();
        pub static SENSOR_ARGS: std::sync::OnceLock<Vec<Box<dyn common::sensor::SensorArgs>>> = std::sync::OnceLock::new();

        pub fn init_sensors() {
            SENSORS.set(vec![#(Box::new(#sensors::#sensors_caps::default()),)*]).unwrap();
            SENSOR_ARGS.set(vec![#(Box::new(#sensors::#sensor_config::default()),)*]).unwrap();
            // hack to prevent serde issues
            #(
                serde_json::to_string(&#sensors::#sensors_caps::default()).unwrap();
            )*
            #(
                serde_json::to_string(&#sensors::#sensor_config::default()).unwrap();
            )*
        }
    }
    .into()
}

#[proc_macro]
pub fn include_plots(_: TokenStream) -> TokenStream {
    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("setup.toml").unwrap()).unwrap();
    let mut plots = Vec::new();
    let mut plots_caps = Vec::new();

    for p in config.plots {
        let p_name_caps = format_ident!("{}", p.to_case(Case::Pascal));
        let p_name = format_ident!("{}", p.replace("-", "_"));

        plots.push(p_name);
        plots_caps.push(p_name_caps);
    }

    quote! {
        pub fn init_plots() {
            #(
                serde_json::to_string(&#plots::#plots_caps::default()).unwrap();
            )*
        }
    }
    .into()
}

#[proc_macro]
pub fn plugin_names_str(_: TokenStream) -> TokenStream {
    let config: BuildConfig =
        toml::from_str(&std::fs::read_to_string("setup.toml").unwrap()).unwrap();

    let mut names = config
        .benches
        .iter()
        .map(|x| x.replace("-", "_"))
        .collect::<Vec<_>>();
    names.extend(config.sensors.iter().map(|x| x.replace("-", "_")));
    names.extend(config.plots.iter().map(|x| x.replace("-", "_")));

    quote! {
        &[#(#names),*]
    }
    .into()
}
