#!/usr/bin/env python3
"""
DuckDB UI Server with DuckLake Setup

This script can:
1. Install DuckDB extensions (for Dockerfile build)
2. Setup DuckLake and start UI server (for runtime)

Usage:
    python start_ui.py install    # Install extensions only
    python start_ui.py start      # Setup DuckLake and start UI server
"""

import argparse
import os
import sys
import time

import duckdb


def install_extensions():
    """Install required DuckDB extensions."""
    extensions = [
        "autocomplete",
        "ducklake",
        "httpfs",
        "icu",
        "json",
        "postgres",
        "ui",
    ]

    print("=" * 60)
    print("Installing DuckDB Extensions")
    print("=" * 60)

    try:
        # Connect to temporary in-memory database
        con = duckdb.connect(":memory:")

        for ext in extensions:
            print(f"Installing {ext}...", end=" ", flush=True)
            con.execute(f"INSTALL {ext}")
            print("✓")

        con.close()

        print("=" * 60)
        print(f"✅ Successfully installed {len(extensions)} extensions")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Failed to install extensions: {e}")
        sys.exit(1)


def setup_ducklake(con):
    """Setup DuckLake with Postgres catalog and S3 storage."""
    print("=" * 60)
    print("Setting up DuckLake...")
    print("=" * 60)

    # Verify required environment variables
    required_env_vars = [
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "URL_STYLE",
        "BUCKET_NAME",
    ]

    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    print(
        f"✓ Connecting to Postgres catalog: {os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
    )
    print(f"✓ Using S3 bucket: s3://{os.getenv('BUCKET_NAME')}")

    setup_sql = f"""
    -- Idempotent cleanup
    DETACH DATABASE IF EXISTS datalake;
    DROP SECRET IF EXISTS __default_postgres;
    DROP SECRET IF EXISTS __default_s3;

    -- Create Postgres secret for catalog
    CREATE SECRET __default_postgres (
        TYPE postgres,
        HOST '{os.getenv("PGHOST")}',
        PORT {os.getenv("PGPORT")},
        DATABASE '{os.getenv("PGDATABASE")}',
        USER '{os.getenv("PGUSER")}',
        PASSWORD '{os.getenv("PGPASSWORD")}'
    );

    -- Create S3 secret for storage
    CREATE SECRET __default_s3 (
        TYPE s3,
        KEY_ID '{os.getenv("AWS_ACCESS_KEY_ID")}',
        SECRET '{os.getenv("AWS_SECRET_ACCESS_KEY")}',
        REGION '{os.getenv("AWS_REGION", "us-east-1")}',
        ENDPOINT '{os.getenv("DUCKDB_S3_ENDPOINT", "")}',
        USE_SSL {os.getenv("DUCKDB_S3_USE_SSL", "TRUE")},
        URL_STYLE '{os.getenv("URL_STYLE", "path")}'
    );

    -- Load extensions (already installed during build)
    LOAD ducklake;
    LOAD postgres;

    -- Attach DuckLake database
    ATTACH 'ducklake:postgres:' AS datalake
       (DATA_PATH 's3://{os.getenv("BUCKET_NAME")}', ENCRYPTED);

    -- Use datalake as default database
    USE datalake;
    """

    try:
        # Execute setup SQL
        con.execute(setup_sql)

        print("=" * 60)
        print("✅ DuckLake setup complete!")
        print("=" * 60)

    except Exception as e:
        print("=" * 60)
        print(f"❌ DuckLake setup failed: {e}")
        print("=" * 60)
        sys.exit(1)


def start_server():
    """Setup DuckLake and start UI server."""
    print("\n" + "=" * 60)
    print("DuckDB UI with DuckLake Setup")
    print("=" * 60)

    # Connect to in-memory database (DuckLake will be attached)
    print("Connecting to DuckDB...")
    con = duckdb.connect(":memory:")
    print("✓ Connected to DuckDB")

    # Setup DuckLake
    setup_ducklake(con)

    # Start UI server
    print("\n" + "=" * 60)
    print("Starting DuckDB UI server...")
    print("=" * 60)

    try:
        # Start the UI server
        con.execute("CALL start_ui_server()")
        pass
    except Exception as e:
        print(f"❌ Failed to start UI server: {e}")
        sys.exit(1)

    # Keep the server running
    print("\nPress Ctrl+C to stop the server...\n")
    sys.stdout.flush()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        con.close()
        print("Goodbye!")


def main():
    """Main entry point with CLI argument handling."""
    parser = argparse.ArgumentParser(description="DuckDB UI Server with DuckLake Setup")
    parser.add_argument(
        "command",
        choices=["install", "start"],
        help="Command to execute: 'install' extensions or 'start' UI server",
    )

    args = parser.parse_args()

    if args.command == "install":
        install_extensions()
    elif args.command == "start":
        start_server()


if __name__ == "__main__":
    main()
