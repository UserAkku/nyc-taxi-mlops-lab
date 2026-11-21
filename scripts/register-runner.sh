#!/usr/bin/env bash
# Register a lab host as a self-hosted GitHub Actions runner, through SSM (no SSH in this lab).
#
#   export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...   (AWS Details)
#   scripts/register-runner.sh            # app host, label lab-app (S0)
#   scripts/register-runner.sh platform   # platform host, label lab-platform (S1); llm in S6
#
# The runner runs as the `runner` user (made by the host's user-data) as a systemd service, so it
# comes back by itself after Start Lab. Its jobs get AWS access from the host's instance profile
# (LabInstanceProfile): no AWS credentials are stored in GitHub for CD.
#
# Safety: the repo is public, so this refuses to run unless fork-PR workflows need approval for all
# external contributors. The registration token (valid one hour, can only register runners) goes from
# gh to a 0600 temp file to SSM; it is never printed or put on a command line. It does appear in the
# SSM command's parameters in this AWS account, and expires within the hour.
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

role="${1:-app}"
case "$role" in app | platform | llm) ;; *) echo "usage: $0 [app|platform|llm]" >&2; exit 2 ;; esac
label="lab-$role"

# Pinned runner release (github.com/actions/runner/releases) and its published SHA-256 (linux-arm64).
RUNNER_VERSION=2.338.2
RUNNER_SHA256=670adb2f268fe21dcbb58b3db0e0aa9d5b3bddfe2c651481336e561ff86de945

here="$(cd "$(dirname "$0")" && pwd)"
aws sts get-caller-identity >/dev/null ||
  { echo "AWS credentials missing or expired: export them from Learner Lab > AWS Details" >&2; exit 1; }
instance=$(terraform -chdir="$here/../infra/live" output -raw "${role}_instance_id") ||
  { echo "no ${role}_instance_id output: apply infra/live first" >&2; exit 1; }
repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)

policy=$(gh api "repos/$repo/actions/permissions/fork-pr-contributor-approval" -q .approval_policy)
if [[ "$policy" != "all_external_contributors" ]]; then
  echo "refusing: fork-PR approval on $repo is '$policy'. Set Settings > Actions > General >" >&2
  echo "'Require approval for all external contributors' first (stages/S0.md, step 4)." >&2
  exit 1
fi

echo "waiting for $instance ($label) to be online in SSM..."
for _ in $(seq 1 60); do
  state=$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$instance" \
    --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)
  [[ "$state" == "Online" ]] && break
  sleep 10
done
[[ "$state" == "Online" ]] || { echo "$instance is not online in SSM after 10 minutes" >&2; exit 1; }

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"
token=$(gh api -X POST "repos/$repo/actions/runners/registration-token" -q .token)

# Runs as root on the host. Idempotent: an already configured runner is left alone (re-register with
# --replace by deleting /opt/actions-runner/.runner first).
read -r -d '' remote <<'EOF' || true
set -euo pipefail
for _ in $(seq 1 60); do [ -f /var/lib/lab-setup-done ] && break; sleep 5; done
[ -f /var/lib/lab-setup-done ] || { echo "user-data has not finished (see /var/log/lab-setup.log)"; exit 1; }
cd /opt/actions-runner
if [ -f .runner ]; then
  echo "runner already configured on $(hostname)"
else
  tgz="actions-runner-linux-arm64-${RUNNER_VERSION}.tar.gz"
  curl -fsSL -o "$tgz" "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/$tgz"
  echo "${RUNNER_SHA256}  $tgz" | sha256sum -c -
  tar xzf "$tgz" && rm -f "$tgz"
  chown -R runner:runner /opt/actions-runner
  sudo -u runner ./config.sh --unattended --replace --url "https://github.com/${REPO}" \
    --token "${TOKEN}" --name "lab-${ROLE}" --labels "${LABEL}" --work _work
fi
[ -f .service ] || ./svc.sh install runner
./svc.sh start || true
./svc.sh status | head -5
EOF

jq -n --arg script "$remote" --arg v "$RUNNER_VERSION" --arg sha "$RUNNER_SHA256" \
  --arg repo "$repo" --arg token "$token" --arg role "$role" --arg label "$label" '{commands: [
    "export RUNNER_VERSION=\($v) RUNNER_SHA256=\($sha) REPO=\($repo) TOKEN=\($token) ROLE=\($role) LABEL=\($label)",
    $script
  ]}' >"$tmp"
unset token

cmd=$(aws ssm send-command --instance-ids "$instance" --document-name AWS-RunShellScript \
  --comment "register $label runner" --parameters "file://$tmp" --query Command.CommandId --output text)
rm -f "$tmp"
echo "registering $label on $instance (SSM command $cmd)..."
# Not `aws ssm wait`: it gives up after ~100 s, and the first boot plus download can take longer.
for _ in $(seq 1 90); do
  sleep 5
  s=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" \
    --query Status --output text 2>/dev/null || echo Pending)
  [[ "$s" == Pending || "$s" == InProgress || "$s" == Delayed ]] || break
done
status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" \
  --query '[Status, StandardOutputContent, StandardErrorContent]' --output text)
echo "$status" | grep -v -i token || true
[[ "$status" == Success* ]] || { echo "registration failed on $instance" >&2; exit 1; }

gh api "repos/$repo/actions/runners" -q ".runners[] | select(.name == \"lab-$role\") | \"\(.name): \(.status) \([.labels[].name] | join(\",\"))\""
