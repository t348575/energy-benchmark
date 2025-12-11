use clap::Parser;
use common::config::Cgroup;
use eyre::{Context, ContextCompat, Result};
use tokio::fs::{create_dir_all, read_to_string, remove_dir};

#[derive(Parser)]
struct Cli {
    #[arg(short, long)]
    config: String,
    #[arg(short, long)]
    name: String,
    #[arg(short, long)]
    device: String,
    #[arg(long, default_value_t = 1)]
    copies: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let cgroup: Cgroup = serde_yml::from_str(
        &read_to_string(&args.config)
            .await
            .context("Reading config file")?,
    )
    .context("Parsing config file")?;

    for i in 0..args.copies {
        let cgroup_path = format!("/sys/fs/cgroup/{}-{}", args.name, i);
        _ = remove_dir(&cgroup_path).await;
        create_dir_all(&cgroup_path)
            .await
            .context("Create cgroup dir")?;
        let device = args
            .device
            .strip_prefix("/dev/")
            .context("Device does not include /dev")?;
        let device = read_to_string(format!("/sys/block/{device}/dev"))
            .await
            .context("Block device does not exist")?;
        cgroup
            .apply(
                &cgroup_path,
                &device,
                if i == args.copies - 1 { true } else { false },
            )
            .await?;
        println!("Created cgroup {cgroup_path}");
    }
    Ok(())
}
