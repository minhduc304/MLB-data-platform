"""
Query today's MLB schedule and dynamically update EventBridge pre-game rules.
Called at the end of the post-game pipeline to schedule today's pre-game runs.
"""
import logging
import sys
from datetime import datetime, timedelta, timezone

import boto3
import statsapi
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — matches the AWS infra set up in the pipeline
# ---------------------------------------------------------------------------
REGION = "us-west-2"
CLUSTER_ARN = "arn:aws:ecs:us-west-2:143136004753:cluster/mlb-pipeline"
PREGAME_TASK_DEF = "arn:aws:ecs:us-west-2:143136004753:task-definition/mlb-pipeline-pregame:1"
SUBNET = "subnet-088e978ee957971c0"
EVENTS_ROLE_ARN = "arn:aws:iam::143136004753:role/mlb-pipeline-events-role"

RULE_1 = "mlb-pipeline-pregame-1"
RULE_2 = "mlb-pipeline-pregame-2"
LEAD_MINUTES = 90       # 1.5 hours before first pitch
COLLAPSE_HOURS = 2      # treat as one wave if games span < 2 hours


def get_today_game_times() -> list[datetime]:
    """Return sorted UTC start times for today's scheduled games."""
    today = datetime.now(tz=timezone.utc).strftime("%m/%d/%Y")
    games = statsapi.schedule(date=today)
    times = []
    for game in games:
        if game.get("status") in ("Scheduled", "Pre-Game", "Warmup"):
            dt_str = game.get("game_datetime")
            if dt_str:
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                times.append(dt)
    return sorted(times)


def compute_triggers(game_times: list[datetime]) -> list[datetime]:
    """
    Return 1 or 2 future trigger times (1.5h before first/last game).
    Collapses to 1 trigger if all games are within 2 hours of each other.
    Returns [] if no upcoming games.
    """
    if not game_times:
        return []

    now = datetime.now(tz=timezone.utc)
    earliest_trigger = game_times[0] - timedelta(minutes=LEAD_MINUTES)
    latest_trigger = game_times[-1] - timedelta(minutes=LEAD_MINUTES)

    span = (game_times[-1] - game_times[0]).total_seconds()
    if span < COLLAPSE_HOURS * 3600:
        candidates = [earliest_trigger]
    else:
        candidates = [earliest_trigger, latest_trigger]

    return [t for t in candidates if t > now]


def to_cron(dt: datetime) -> str:
    """Convert a UTC datetime to an EventBridge cron expression."""
    return f"cron({dt.minute} {dt.hour} {dt.day} {dt.month} ? {dt.year})"


def enable_rule(rule_name: str, cron_expr: str) -> None:
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name=rule_name, ScheduleExpression=cron_expr, State="ENABLED")
    events.put_targets(
        Rule=rule_name,
        Targets=[{
            "Id": f"{rule_name}-target",
            "Arn": CLUSTER_ARN,
            "RoleArn": EVENTS_ROLE_ARN,
            "EcsParameters": {
                "TaskDefinitionArn": PREGAME_TASK_DEF,
                "TaskCount": 1,
                "LaunchType": "FARGATE",
                "NetworkConfiguration": {
                    "awsvpcConfiguration": {
                        "Subnets": [SUBNET],
                        "AssignPublicIp": "ENABLED",
                    }
                },
            },
        }],
    )
    logger.info(f"Enabled {rule_name}: {cron_expr}")


def disable_rule(rule_name: str) -> None:
    events = boto3.client("events", region_name=REGION)
    try:
        events.disable_rule(Name=rule_name)
        logger.info(f"Disabled {rule_name} — not needed today")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise


if __name__ == "__main__":
    game_times = get_today_game_times()
    logger.info(f"Found {len(game_times)} upcoming games today")

    triggers = compute_triggers(game_times)

    if not triggers:
        logger.info("No upcoming games — disabling both pre-game rules")
        disable_rule(RULE_1)
        disable_rule(RULE_2)
        sys.exit(0)

    enable_rule(RULE_1, to_cron(triggers[0]))

    if len(triggers) > 1:
        enable_rule(RULE_2, to_cron(triggers[1]))
    else:
        disable_rule(RULE_2)

    logger.info("Pre-game schedule updated successfully")
