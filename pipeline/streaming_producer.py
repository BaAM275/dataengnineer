import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Generator, Optional

from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
import os


TOUR_REPOS = {
    "atp": "JeffSackmann/tennis_atp",
    "wta": "JeffSackmann/tennis_wta",
}


def create_pubsub_topic(project_id: str, topic_id: str) -> None:
    """Create Pub/Sub topic if it doesn't exist."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"Created topic: {topic_path}")
    except AlreadyExists:
        print(f"Topic {topic_path} already exists")


def create_pubsub_subscription(
    project_id: str, topic_id: str, subscription_id: str
) -> None:
    """Create Pub/Sub subscription if it doesn't exist."""
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    try:
        subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": 60,
                "enable_exactly_once_delivery": True,
            }
        )
        print(f"Created subscription: {subscription_path}")
    except AlreadyExists:
        print(f"Subscription {subscription_path} already exists")


def publish_match_to_pubsub(
    project_id: str,
    topic_id: str,
    match_data: dict,
    attributes: Optional[dict] = None,
) -> str:
    """
    Publish a single match record to Pub/Sub topic.

    Args:
        project_id: GCP project ID
        topic_id: Pub/Sub topic ID
        match_data: Dictionary containing match data
        attributes: Optional message attributes

    Returns:
        Message ID of the published message
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    message_data = json.dumps(match_data).encode("utf-8")

    message_attributes = attributes or {}
    message_attributes.update({
        "published_at": datetime.utcnow().isoformat(),
        "data_version": "2.0",
    })

    future = publisher.publish(topic_path, message_data, **message_attributes)
    message_id = future.result(timeout=30)
    return message_id


def read_csv_matches(file_path: Path) -> Generator[dict, None, None]:
    """
    Read matches from a CSV file and yield as dictionaries.

    Args:
        file_path: Path to the CSV file

    Yields:
        Dictionary for each row in the CSV
    """
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def publish_csv_to_pubsub(
    project_id: str,
    topic_id: str,
    csv_path: Path,
    batch_size: int = 100,
    attributes: Optional[dict] = None,
) -> int:
    """
    Publish all matches from a CSV file to Pub/Sub.

    Args:
        project_id: GCP project ID
        topic_id: Pub/Sub topic ID
        csv_path: Path to the CSV file
        batch_size: Number of messages to batch before publishing
        attributes: Optional message attributes

    Returns:
        Number of messages published
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    message_count = 0
    batch = []

    for match in read_csv_matches(csv_path):
        message_data = json.dumps(match).encode("utf-8")

        msg_attributes = (attributes or {}).copy()
        msg_attributes.update({
            "published_at": datetime.utcnow().isoformat(),
            "data_version": "2.0",
            "source_file": str(csv_path.name),
        })

        batch.append({
            "data": message_data,
            "attributes": msg_attributes,
        })

        if len(batch) >= batch_size:
            _publish_batch(publisher, topic_path, batch)
            message_count += len(batch)
            batch = []

    if batch:
        _publish_batch(publisher, topic_path, batch)
        message_count += len(batch)

    return message_count


def _publish_batch(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    batch: list,
) -> None:
    """Publish a batch of messages."""
    futures = []

    for msg in batch:
        future = publisher.publish(
            topic_path,
            msg["data"],
            **msg["attributes"]
        )
        futures.append(future)

    for future in futures:
        future.result(timeout=60)


def publish_tour_data_streaming(
    project_id: str,
    topic_id: str,
    csv_paths: list[Path],
    batch_size: int = 100,
) -> dict:
    """
    Publish all CSV data to Pub/Sub for streaming pipeline.

    Args:
        project_id: GCP project ID
        topic_id: Pub/Sub topic ID
        csv_paths: List of CSV file paths
        batch_size: Number of messages to batch

    Returns:
        Summary of published messages
    """
    total_published = 0
    tour_counts = {}

    for csv_path in csv_paths:
        tour = csv_path.stem.split("_")[0].upper()
        year = csv_path.stem.split("_")[-1]

        attributes = {
            "tour": tour,
            "year": year,
            "pipeline_version": "2.0",
        }

        count = publish_csv_to_pubsub(
            project_id=project_id,
            topic_id=topic_id,
            csv_path=csv_path,
            batch_size=batch_size,
            attributes=attributes,
        )

        total_published += count
        tour_counts[tour] = tour_counts.get(tour, 0) + count
        print(f"Published {count} messages from {csv_path.name}")

    return {
        "total_published": total_published,
        "tour_counts": tour_counts,
        "timestamp": datetime.utcnow().isoformat(),
    }


def publish_realtime_match(
    project_id: str,
    topic_id: str,
    match_data: dict,
) -> str:
    """
    Publish a single real-time match update.

    This function simulates publishing live match data (e.g., from a web scraper
    or external API) to the streaming pipeline.

    Args:
        project_id: GCP project ID
        topic_id: Pub/Sub topic ID
        match_data: Dictionary containing match data

    Returns:
        Message ID of the published message
    """
    attributes = {
        "is_realtime": "true",
        "published_at": datetime.utcnow().isoformat(),
        "data_version": "2.0",
    }

    message_id = publish_match_to_pubsub(
        project_id=project_id,
        topic_id=topic_id,
        match_data=match_data,
        attributes=attributes,
    )

    print(f"Published realtime match: {message_id}")
    return message_id


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Publish tennis data to Pub/Sub")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--topic-id", default="tennis-matches-stream", help="Pub/Sub topic ID")
    parser.add_argument("--csv-path", help="Path to CSV file to publish")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for publishing")

    args = parser.parse_args()

    if args.csv_path:
        csv_path = Path(args.csv_path)
        count = publish_csv_to_pubsub(
            project_id=args.project_id,
            topic_id=args.topic_id,
            csv_path=csv_path,
            batch_size=args.batch_size,
        )
        print(f"Published {count} messages")
