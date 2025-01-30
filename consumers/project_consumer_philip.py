"""
project_consumer_philip.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

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
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt
import pandas as pd


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
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("PROJECT_TOPIC", "buzzline-topic")
    return group_id


#####################################
# Set up data structures
#####################################

# Initialize a dataframe to store data
data = pd.DataFrame(columns=["message", "author", "timestamp", "category", "sentiment", "keyword_mentioned", "message_length"])

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """Update the live chart with the average sentiment for each author."""
    # Clear the previous chart
    ax.clear()

    # Group the data by author and calculate the average sentiment for each author
    avg_sentiment = data.groupby("author")["sentiment"].mean().reset_index()

    # Create a bar chart using the bar() method.
    # Pass in the author list and the average sentiment list
    ax.bar(avg_sentiment["author"], avg_sentiment["sentiment"], color="skyblue")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Author")
    ax.set_ylabel("Average Sentiment")
    ax.set_title("Real-Time Average Sentiment by Author")

    # Use the set_xticklabels() method to rotate the x-axis labels
    # Pass in the x list, specify the rotation angle is 45 degrees,
    # and align them to the right
    # ha stands for horizontal alignment
    ax.set_xticklabels("author", rotation=45, ha="right")

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)


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

        # Parse the JSON string into a dictionary
        message_dict = json.loads(message)

        # Append the message to the dataframe
        data.loc[len(data)] = [
            message_dict.get("message", ""),
            message_dict.get("author", "unknown"),
            message_dict.get("timestamp", ""),
            message_dict.get("category", ""),
            message_dict.get("sentiment", 0.0),
            message_dict.get("keyword_mentioned", ""),
            message_dict.get("message_length", 0)
        ]

    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON message: {e}")


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

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()
