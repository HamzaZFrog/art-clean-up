import subprocess
import argparse
import json
import os
import sys
import fnmatch
import uuid
import logging
import csv
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

def setup_logger():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"clean_old_artifacts_{timestamp}.log"
    logger = logging.getLogger("clean_old_artifacts")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger, timestamp

def jfrog_cli_configure(server_id, url, token, logger):
    logger.info(f"Configuring JFrog CLI with server ID '{server_id}'...")
    try:
        result = subprocess.run(
            ["jf", "config", "show"],
            capture_output=True, text=True, check=True
        )
        if server_id in result.stdout:
            logger.info(f"Server '{server_id}' already configured, skipping add.")
            return
    except subprocess.CalledProcessError:
        pass

    cmd = [
        "jf", "config", "add", server_id,
        "--url", url,
        "--access-token", token,
        "--interactive=false"
    ]

    try:
        subprocess.run(cmd, check=True)
        logger.info(f"Successfully added JFrog CLI server '{server_id}'.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to configure JFrog CLI: {e}")
        sys.exit(1)

    try:
        subprocess.run(["jf", "config", "use", server_id], check=True)
        logger.info(f"Using '{server_id}' as default server.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to set default server: {e}")
        sys.exit(1)

def load_exclusion_patterns(json_path, logger):
    if not os.path.exists(json_path):
        logger.error(f"Exclusion file not found: {json_path}")
        sys.exit(1)
    with open(json_path, "r") as f:
        exclusions = json.load(f)
    patterns = exclusions.get("exclude", [])
    logger.info(f"Loaded {len(patterns)} exclusion patterns from {json_path}")
    return patterns

def is_excluded(full_path, exclusion_patterns, matched_patterns):
    for pattern in exclusion_patterns:
        if fnmatch.fnmatch(full_path, pattern):
            matched_patterns.add(pattern)
            return True
    return False

def get_repositories(logger):
    logger.info("Fetching repository configurations...")
    try:
        result = subprocess.run(
            ["jf", "rt", "curl", "-XGET", "/api/repositories/configurations"],
            capture_output=True, text=True, check=True
        )
        data = json.loads(result.stdout)
        repos = []
        for rclass in ["LOCAL", "FEDERATED"]:
            for repo in data.get(rclass, []):
                repos.append({
                    "key": repo["key"],
                    "class": repo["rclass"]
                })
        return repos
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch repositories: {e}")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error("Failed to parse repository list JSON.")
        sys.exit(1)

def print_table(headers, rows, logger):
    col_widths = [max(len(str(cell)) for cell in [header] + [row[i] for row in rows]) for i, header in enumerate(headers)]
    line = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    header_row = "| " + " | ".join(f"{headers[i]:<{col_widths[i]}}" for i in range(len(headers))) + " |"

    logger.info(line)
    logger.info(header_row)
    logger.info(line)
    for row in rows:
        row_str = "| " + " | ".join(f"{str(row[i]):<{col_widths[i]}}" for i in range(len(row))) + " |"
        logger.info(row_str)
    logger.info(line)

def get_old_artifacts(spec_path, spec_timeframe, repo, logger):
    if not os.path.exists(spec_path):
        logger.error(f"AQL spec file not found: {spec_path}")
        sys.exit(1)

    spec_vars = f"timeframe={spec_timeframe};repo={repo}"
    command = ["jf", "rt", "search", "--spec", spec_path, "--spec-vars", spec_vars]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error("Search using spec file failed:")
        logger.error(e.stderr.strip())
        return None

def parse_artifacts(search_output, logger):
    try:
        data = json.loads(search_output)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "results" in data:
            return data["results"]
        else:
            logger.warning("Unexpected data format in search output.")
            return []
    except json.JSONDecodeError:
        logger.error("Failed to parse search response.")
        return []

def build_delete_command(path, dry_run):
    cmd = ["jf", "rt", "del", path, "--quiet"]
    if dry_run:
        cmd.append("--dry-run")
    return cmd

def execute_delete(cmd, logger):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if "--dry-run" in cmd:
            logger.info(f"[DRYRUN-COMPLETE] {' '.join(cmd)}")
        else:
            logger.info(f"[DELETED] {' '.join(cmd)}")
        return True, ""
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip() if e.stderr else "Unknown error"
        logger.error(f"[ERROR] Delete failed: {' '.join(cmd)} - {error_msg}")
        return False, error_msg

def main():
    logger, timestamp = setup_logger()

    parser = argparse.ArgumentParser(description="Delete old artifacts from JFrog repositories.")
    parser.add_argument("--artifactory-url", required=True, help="Artifactory base URL")
    parser.add_argument("--access-token", required=True, help="JFrog access token")
    parser.add_argument("--older-than", required=True,
                        help="Retention window (e.g. 90d, 3mo, 1y)")
    parser.add_argument("--exclusions-file", required=True,
                        help="Path to exclusions JSON file")
    parser.add_argument("--aql-spec", required=True,
                        help="Path to AQL spec file")
    parser.add_argument("--dry-run", action="store_true",
                        help="List deletions without executing them")
    parser.add_argument("--threads", type=int, default=4,
                        help="Number of parallel threads to use for deletion")

    args = parser.parse_args()

    server_id = f"cli-config-{uuid.uuid4().hex[:8]}"
    jfrog_cli_configure(server_id, args.artifactory_url, args.access_token, logger)

    exclusion_patterns = load_exclusion_patterns(args.exclusions_file, logger)

    repos = get_repositories(logger)
    if not repos:
        logger.info("No LOCAL or FEDERATED repositories found.")
        return

    logger.info("Repositories discovered:")
    repo_rows = [(r["key"], r["class"]) for r in repos]
    print_table(["Repository", "Class"], repo_rows, logger)

    csv_filename = f"clean-up-{timestamp}.csv"
    csv_records = []

    for repo in repos:
        logger.info(f"Processing repository: {repo['key']} ({repo['class']})")
        raw_output = get_old_artifacts(args.aql_spec, args.older_than, repo["key"], logger)
        if not raw_output:
            continue

        artifacts = parse_artifacts(raw_output, logger)
        if not artifacts:
            logger.info(f"No matching artifacts found in {repo['key']}.")
            continue

        logger.info(f"{len(artifacts)} artifact(s) found in {repo['key']}. Checking exclusions...")

        delete_commands = []

        for item in artifacts:
            path = item.get("path", "")
            matched_patterns = set()
            if is_excluded(path, exclusion_patterns, matched_patterns):
                logger.info(f"[SKIP] Excluded by pattern: {path}")
                csv_records.append({
                    "Repository": repo["key"],
                    "Path": path,
                    "Status": "skipped",
                    "Exclusion Pattern": ", ".join(matched_patterns),
                    "Error": ""
                })
                continue

            cmd = build_delete_command(path, args.dry_run)
            delete_commands.append(cmd)

        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            future_to_cmd = {executor.submit(execute_delete, cmd, logger): cmd for cmd in delete_commands}
            for future in as_completed(future_to_cmd):
                cmd = future_to_cmd[future]
                try:
                    success, error_msg = future.result()
                    status = "deleted" if success else "error"
                    csv_records.append({
                        "Repository": repo["key"],
                        "Path": cmd[3],  # path
                        "Status": status,
                        "Exclusion Pattern": "",
                        "Error": error_msg
                    })
                except Exception as exc:
                    logger.error(f"Unexpected error while deleting {cmd[3]}: {exc}")
                    csv_records.append({
                        "Repository": repo["key"],
                        "Path": cmd[3],
                        "Status": "Error",
                        "Exclusion Pattern": "",
                        "Error": str(exc)
                    })

    # Write CSV
    csv_headers = ["Repository", "Path", "Status", "Exclusion Pattern", "Error"]
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers, delimiter=';')
        writer.writeheader()
        for row in csv_records:
            writer.writerow(row)

    logger.info(f"CSV report generated: {csv_filename}")

if __name__ == "__main__":
    main()
