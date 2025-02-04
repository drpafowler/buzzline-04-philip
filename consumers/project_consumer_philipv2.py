"""
project_consumer_philipv2.py

Consume json messages from a Kafka topic and save them to a database.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
import sqlite3 as sql

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("PROJECT_TOPIC", "buzzline-topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("PROJECT_TOPIC", "buzzline-topic")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# No need for data structures if saving directly to a database

#####################################
# Set up live visuals
#####################################

# uneeded for saving to a database

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_db():
    # Try to connect to the database, create if it does not exist
    try:
        conn = sql.connect('data/buzzline.db')
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Optional: Auto-incrementing ID for each message
            message TEXT NOT NULL,
            author TEXT NOT NULL,
            timestamp TEXT NOT NULL,  -- Store as TEXT (ISO 8601 format is recommended)
            category TEXT,
            sentiment REAL,  -- Use REAL for floating-point sentiment scores
            keyword_mentioned TEXT,  -- Can be NULL if no keyword is mentioned
            message_length INTEGER
        );
        ''')
        conn.commit()
        conn.close()
    except sql.OperationalError:
        logger.info("Database already exists.")



#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):

            # Extract all fields from the message and send to the database
            message = message_dict.get("message", "unknown")
            author = message_dict.get("author", "unknown")
            timestamp = message_dict.get("timestamp", "unknown")
            category = message_dict.get("category", "unknown")
            sentiment = message_dict.get("sentiment", "unknown")
            keyword_mentioned = message_dict.get("keyword_mentioned", "unknown")
            message_length = message_dict.get("message_length", "unknown")

            # log the message received
            logger.info(f"Message received from author: {author} in category: {category}")

            # Insert the extracted fields into the database
            try:
                conn = sql.connect('data/buzzline.db')
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO messages (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (message, author, timestamp, category, sentiment, keyword_mentioned, message_length))
                conn.commit()
                conn.close()
            except sql.Error as e:
                logger.error(f"Error inserting into database: {e}")

            # Log the updated database
            logger.info(f"Database updated successfully for message: {message}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # Call update_db to ensure the database and table are created
    update_db()

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # message is a complex object with metadata and value
            # Use the value attribute to extract the message as a string
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":

    # Call the main function to start the consumer
    main()


