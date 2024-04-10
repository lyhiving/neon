use anyhow::bail;
use pageserver_api::shard::TenantShardId;
use s3_scrubber::garbage::{find_garbage, purge_garbage, PurgeMode};
use s3_scrubber::scan_pageserver_metadata::scan_metadata;
use s3_scrubber::{
    init_logging, scan_safekeeper_metadata::scan_safekeeper_metadata, BucketConfig, ConsoleConfig,
    NodeKind, TraversingDepth,
};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short, long, default_value_t = false)]
    delete: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    FindGarbage {
        #[arg(short, long)]
        node_kind: NodeKind,
        #[arg(short, long, default_value_t=TraversingDepth::Tenant)]
        depth: TraversingDepth,
        #[arg(short, long, default_value_t = String::from("garbage.json"))]
        output_path: String,
    },
    PurgeGarbage {
        #[arg(short, long)]
        input_path: String,
        #[arg(short, long, default_value_t = PurgeMode::DeletedOnly)]
        mode: PurgeMode,
    },
    #[command(verbatim_doc_comment)]
    ScanMetadata {
        #[arg(short, long)]
        node_kind: NodeKind,
        #[arg(short, long, default_value_t = false)]
        json: bool,
        #[arg(long = "tenant-id", num_args = 0..)]
        tenant_ids: Vec<TenantShardId>,
        #[arg(long, default_value = None)]
        /// For safekeeper node_kind only, points to db with debug dump
        dump_db_connstr: Option<String>,
        /// For safekeeper node_kind only, table in the db with debug dump
        #[arg(long, default_value = None)]
        dump_db_table: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let bucket_config = BucketConfig::from_env()?;

    let command_log_name = match &cli.command {
        Command::ScanMetadata { .. } => "scan",
        Command::FindGarbage { .. } => "find-garbage",
        Command::PurgeGarbage { .. } => "purge-garbage",
    };
    let _guard = init_logging(&format!(
        "{}_{}_{}_{}.log",
        std::env::args().next().unwrap(),
        command_log_name,
        bucket_config.bucket,
        chrono::Utc::now().format("%Y_%m_%d__%H_%M_%S")
    ));

    match cli.command {
        Command::ScanMetadata {
            json,
            tenant_ids,
            node_kind,
            dump_db_connstr,
            dump_db_table,
        } => {
            if let NodeKind::Safekeeper = node_kind {
                let dump_db_connstr =
                    dump_db_connstr.ok_or(anyhow::anyhow!("dump_db_connstr not specified"))?;
                let dump_db_table =
                    dump_db_table.ok_or(anyhow::anyhow!("dump_db_table not specified"))?;

                let summary = scan_safekeeper_metadata(
                    bucket_config.clone(),
                    tenant_ids.iter().map(|tshid| tshid.tenant_id).collect(),
                    dump_db_connstr,
                    dump_db_table,
                )
                .await?;
                if json {
                    println!("{}", serde_json::to_string(&summary).unwrap())
                } else {
                    println!("{}", summary.summary_string());
                }
                if summary.is_fatal() {
                    bail!("Fatal scrub errors detected");
                }
                if summary.is_empty() {
                    // Strictly speaking an empty bucket is a valid bucket, but if someone ran the
                    // scrubber they were likely expecting to scan something, and if we see no timelines
                    // at all then it's likely due to some configuration issues like a bad prefix
                    bail!(
                        "No timelines found in bucket {} prefix {}",
                        bucket_config.bucket,
                        bucket_config
                            .prefix_in_bucket
                            .unwrap_or("<none>".to_string())
                    );
                }
                Ok(())
            } else {
                match scan_metadata(bucket_config.clone(), tenant_ids).await {
                    Err(e) => {
                        tracing::error!("Failed: {e}");
                        Err(e)
                    }
                    Ok(summary) => {
                        if json {
                            println!("{}", serde_json::to_string(&summary).unwrap())
                        } else {
                            println!("{}", summary.summary_string());
                        }
                        if summary.is_fatal() {
                            Err(anyhow::anyhow!("Fatal scrub errors detected"))
                        } else if summary.is_empty() {
                            // Strictly speaking an empty bucket is a valid bucket, but if someone ran the
                            // scrubber they were likely expecting to scan something, and if we see no timelines
                            // at all then it's likely due to some configuration issues like a bad prefix
                            Err(anyhow::anyhow!(
                                "No timelines found in bucket {} prefix {}",
                                bucket_config.bucket,
                                bucket_config
                                    .prefix_in_bucket
                                    .unwrap_or("<none>".to_string())
                            ))
                        } else {
                            Ok(())
                        }
                    }
                }
            }
        }
        Command::FindGarbage {
            node_kind,
            depth,
            output_path,
        } => {
            let console_config = ConsoleConfig::from_env()?;
            find_garbage(bucket_config, console_config, depth, node_kind, output_path).await
        }
        Command::PurgeGarbage { input_path, mode } => {
            purge_garbage(input_path, mode, !cli.delete).await
        }
    }
}
