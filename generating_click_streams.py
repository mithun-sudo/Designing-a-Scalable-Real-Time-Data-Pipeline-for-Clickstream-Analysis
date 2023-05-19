from random import *
from datetime import datetime, timedelta
from kafka import KafkaProducer
from time import sleep
from json import dumps


def generate_random_ip():
    """Generates random ip address"""
    ip = ".".join(str(randint(0, 255)) for _ in range(4))
    return ip


"""Kafka server is running on localhost:9092. Created a producer that writes data to kafka server. Converts python object
to json string which is encoded to binary format."""
streaming = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))
click_event_id = 1


"""A while loop that streams click data endlessly."""
while True:
    """ Using random module, randomly generating geo data of a user."""
    location = {
        "USA": ["New York City", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"],
        "India": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Ahmedabad", "Pune", "Jaipur", "Surat"],
        "England": ["London", "Birmingham", "Manchester", "Leeds", "Liverpool", "Newcastle", "Sheffield", "Bristol", "Nottingham", "Leicester"],
        "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Zaragoza", "Málaga", "Murcia", "Palma", "Bilbao", "Alicante"],
        "Italy": ["Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa", "Bologna", "Florence", "Bari", "Catania"]
    }
    user_country = choice(list(location.keys()))
    user_city = choice(location[user_country])
    user_ip_address = generate_random_ip()
    geo_data = {
        "country": user_country,
        "city": user_city,
        "ip_address": user_ip_address
    }


    """ Using random module, randomly generating user agent data of a user."""
    device = ["Laptop", "Mobile"]
    laptop_browser = ["Chrome", "Firefox", "Safari", "Opera"]
    mobile_browser = ["Chrome Mobile", "Safari", "Firefox Mobile", "Opera Mini"]
    user_device = choice(device)
    if user_device == 'Laptop':
        user_browser = choice(laptop_browser)
    else:
        user_browser = choice(mobile_browser)
    user_agent_data = {
        "device": user_device,
        "browser": user_browser
    }
    user_id = randint(525572, 525872)

    """Assigning the number of clicks the user is going to perform on the website beforehand."""
    amount_of_clicks_in_a_session = randint(1, 10) + 1

    for i in range(amount_of_clicks_in_a_session):
        if i != amount_of_clicks_in_a_session - 1:
            """Assuming the website has a total of 50 products, this section of code decides which page url the user is going 
            to click and how much time he is going to spend in each webpage."""
            webpage_url = f"www.globalmart.com/product-id-{randint(1, 50)}"
            minutes_spent_in_webpage = randint(2, 6)
            seconds_spent_in_webpage = randint(1, 60)
            current_timestamp = datetime.now() + timedelta(minutes=minutes_spent_in_webpage, seconds=seconds_spent_in_webpage)
            click_data = {"user_id": user_id, "timestamp": str(current_timestamp)[:19], "url": webpage_url}

            """Click event data:"""
            click_stream = {
                "click_event_id": click_event_id,
                "click_data": click_data,
                "geo_data": geo_data,
                "user_agent_data": user_agent_data
            }
            print(click_stream)

            """The data of click event is written under the topic stream with the help of kafka producer."""
            streaming.send(topic='stream', value=click_stream)
            sleep(1)
            click_event_id += 1
        else:
            """If...else conditional block is used to find out when user has logged out of the webpage."""
            webpage_url = "www.globalmart.com/logout"
            minutes_spent_in_webpage = randint(2, 6)
            seconds_spent_in_webpage = randint(1, 60)
            current_timestamp = current_timestamp + timedelta(minutes=minutes_spent_in_webpage,
                                                              seconds=seconds_spent_in_webpage)
            click_data = {"user_id": user_id, "timestamp": str(current_timestamp)[:19], "url": webpage_url}
            click_stream = {
                "click_event_id": click_event_id,
                "click_data": click_data,
                "geo_data": geo_data,
                "user_agent_data": user_agent_data
            }
            print(click_stream)
            streaming.send(topic='stream', value=click_stream)
            sleep(1)
            click_event_id += 1
