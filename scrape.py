import requests
import json
import time
import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from shapely.geometry import Point, Polygon
from decimal import Decimal
from discord_webhook import DiscordWebhook, DiscordEmbed
import os
from datetime import datetime, timedelta, date, timezone
import calendar
from pytz import timezone as pytz_timezone
import logging
import random

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # Logs to the console
    ]
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

polygon_Edmonton = Polygon([
   (53.24836842, -113.18427309),
    (53.22671673, -113.56849409),
    (53.23791725, -113.69823105),
    (53.31474832, -113.91903339),
    (53.4167272, -114.03255323),
    (53.55553094, -114.03130576),
    (53.68279995, -114.00885128),
    (53.85974393, -113.84293767),
    (53.87592649, -113.53855479),
    (53.87739732, -113.2903081),
    (53.78315977, -113.07075324),
    (53.5762753, -113.01461706),
    (53.24836842, -113.18427309),
])

polygon_NorthOfEdmonton = Polygon([
    (53.50243339, -110.00377323),
    (53.5762753, -113.01461706),
    (53.78315977, -113.07075324),
    (53.87739732, -113.2903081),
    (53.87592649, -113.53855479),
    (53.85974393, -113.84293767),
    (53.68279995, -114.00885128),
    (53.55553094, -114.03130576),
    (53.59736172, -119.90627337),
    (53.85107632, -120.01788168),
    (54.96100652, -119.98712166),
    (59.99900962, -119.95504949),
    (60.02591534, -109.9782346),
    (54.57031627, -109.9782346),
    (54.0463325, -110.01212127),
    (53.50243339, -110.00377323),
])

polygon_SouthOfEdmonton = Polygon([
    (53.55553094, -114.03130576),
    (53.4167272, -114.03255323),
    (53.31474832, -113.91903339),
    (53.23791725, -113.69823105),
    (53.22671673, -113.56849409),
    (53.24836842, -113.18427309),
    (53.5762753, -113.01461706),
    (53.50243339, -110.00377323),
    (53.31762057, -110.00096103),
    (53.21968902, -110.00209789),
    (53.1475128, -110.00209789),
    (52.84412248, -110.00337243),
    (48.99913055, -110.00270566),
    (48.99680373, -111.69899418),
    (48.99859613, -113.48778087),
    (49.00466077, -114.06914793),
    (49.051805, -114.05065923),
    (49.07939732, -114.1276955),
    (49.14058405, -114.16467291),
    (49.15872384, -114.16467291),
    (49.19162724, -114.23451913),
    (49.17551402, -114.28587665),
    (49.21310337, -114.39989033),
    (49.24597062, -114.38345592),
    (49.26474209, -114.44713924),
    (49.38858851, -114.57245158),
    (49.45339973, -114.61251044),
    (49.51378852, -114.58110718),
    (49.56678001, -114.58477738),
    (49.55478748, -114.63818919),
    (49.55945159, -114.73474132),
    (49.62070996, -114.74295852),
    (49.64598988, -114.66181365),
    (49.70647531, -114.65975935),
    (49.74100465, -114.62586339),
    (49.7695392, -114.6546236),
    (49.96947183, -114.67413945),
    (50.38650836, -114.81485904),
    (50.57409707, -115.02747915),
    (50.52841237, -115.2062033),
    (50.71999074, -115.3633573),
    (50.73107632, -115.43408915),
    (50.85002018, -115.63510018),
    (50.90753636, -115.58134141),
    (51.01353362, -115.67717225),
    (51.08516826, -115.79283366),
    (51.06824959, -116.06716859),
    (51.37960225, -116.37162475),
    (51.40963136, -116.31896794),
    (52.28018639, -117.7493307),
    (52.7667831, -118.456811),
    (52.93860097, -118.60292107),
    (53.25260316, -118.99511124),
    (53.22498922, -119.34885139),
    (53.5186203, -119.87177161),
    (53.59736172, -119.90627337),
    (53.55553094, -114.03130576),
    (51.21463168, -114.5251865),
    (51.06945384, -114.6147229),
    (50.88422136, -114.61123447),
    (50.73874109, -114.47053442),
    (50.7107688, -114.01587559),
    (50.72181249, -113.77401105),
    (50.84018427, -113.66005564),
    (51.01681467, -113.46005227),
    (51.11035407, -113.52633246),
    (51.2546757, -113.65191597),
    (51.33102662, -113.87401274),
    (51.3259405, -113.99610782),
    (51.30268252, -114.27285667),
    (51.21463168, -114.5251865),
])

polygon_Calgary = Polygon([
    (50.72181249, -113.77401105),
    (50.7107688, -114.01587559),
    (50.73874109, -114.47053442),
    (50.88422136, -114.61123447),
    (51.06945384, -114.6147229),
    (51.21463168, -114.5251865),
    (51.30268252, -114.27285667),
    (51.3259405, -113.99610782),
    (51.33102662, -113.87401274),
    (51.2546757, -113.65191597),
    (51.11035407, -113.52633246),
    (51.01681467, -113.46005227),
    (50.84018427, -113.66005564),
    (50.72181249, -113.77401105),
])
# Define the coordinates of your polygon

polygon_Edmonton = Polygon([
   (53.24836842, -113.18427309),
    (53.22671673, -113.56849409),
    (53.23791725, -113.69823105),
    (53.31474832, -113.91903339),
    (53.4167272, -114.03255323),
    (53.55553094, -114.03130576),
    (53.68279995, -114.00885128),
    (53.85974393, -113.84293767),
    (53.87592649, -113.53855479),
    (53.87739732, -113.2903081),
    (53.78315977, -113.07075324),
    (53.5762753, -113.01461706),
    (53.24836842, -113.18427309),
])

polygon_NorthOfEdmonton = Polygon([
    (53.50243339, -110.00377323),
    (53.5762753, -113.01461706),
    (53.78315977, -113.07075324),
    (53.87739732, -113.2903081),
    (53.87592649, -113.53855479),
    (53.85974393, -113.84293767),
    (53.68279995, -114.00885128),
    (53.55553094, -114.03130576),
    (53.59736172, -119.90627337),
    (53.85107632, -120.01788168),
    (54.96100652, -119.98712166),
    (59.99900962, -119.95504949),
    (60.02591534, -109.9782346),
    (54.57031627, -109.9782346),
    (54.0463325, -110.01212127),
    (53.50243339, -110.00377323),
])

polygon_SouthOfEdmonton = Polygon([
    (53.55553094, -114.03130576),
    (53.4167272, -114.03255323),
    (53.31474832, -113.91903339),
    (53.23791725, -113.69823105),
    (53.22671673, -113.56849409),
    (53.24836842, -113.18427309),
    (53.5762753, -113.01461706),
    (53.50243339, -110.00377323),
    (53.31762057, -110.00096103),
    (53.21968902, -110.00209789),
    (53.1475128, -110.00209789),
    (52.84412248, -110.00337243),
    (48.99913055, -110.00270566),
    (48.99680373, -111.69899418),
    (48.99859613, -113.48778087),
    (49.00466077, -114.06914793),
    (49.051805, -114.05065923),
    (49.07939732, -114.1276955),
    (49.14058405, -114.16467291),
    (49.15872384, -114.16467291),
    (49.19162724, -114.23451913),
    (49.17551402, -114.28587665),
    (49.21310337, -114.39989033),
    (49.24597062, -114.38345592),
    (49.26474209, -114.44713924),
    (49.38858851, -114.57245158),
    (49.45339973, -114.61251044),
    (49.51378852, -114.58110718),
    (49.56678001, -114.58477738),
    (49.55478748, -114.63818919),
    (49.55945159, -114.73474132),
    (49.62070996, -114.74295852),
    (49.64598988, -114.66181365),
    (49.70647531, -114.65975935),
    (49.74100465, -114.62586339),
    (49.7695392, -114.6546236),
    (49.96947183, -114.67413945),
    (50.38650836, -114.81485904),
    (50.57409707, -115.02747915),
    (50.52841237, -115.2062033),
    (50.71999074, -115.3633573),
    (50.73107632, -115.43408915),
    (50.85002018, -115.63510018),
    (50.90753636, -115.58134141),
    (51.01353362, -115.67717225),
    (51.08516826, -115.79283366),
    (51.06824959, -116.06716859),
    (51.37960225, -116.37162475),
    (51.40963136, -116.31896794),
    (52.28018639, -117.7493307),
    (52.7667831, -118.456811),
    (52.93860097, -118.60292107),
    (53.25260316, -118.99511124),
    (53.22498922, -119.34885139),
    (53.5186203, -119.87177161),
    (53.59736172, -119.90627337),
    (53.55553094, -114.03130576),
    (51.21463168, -114.5251865),
    (51.06945384, -114.6147229),
    (50.88422136, -114.61123447),
    (50.73874109, -114.47053442),
    (50.7107688, -114.01587559),
    (50.72181249, -113.77401105),
    (50.84018427, -113.66005564),
    (51.01681467, -113.46005227),
    (51.11035407, -113.52633246),
    (51.2546757, -113.65191597),
    (51.33102662, -113.87401274),
    (51.3259405, -113.99610782),
    (51.30268252, -114.27285667),
    (51.21463168, -114.5251865),
])

polygon_Calgary = Polygon([
    (50.72181249, -113.77401105),
    (50.7107688, -114.01587559),
    (50.73874109, -114.47053442),
    (50.88422136, -114.61123447),
    (51.06945384, -114.6147229),
    (51.21463168, -114.5251865),
    (51.30268252, -114.27285667),
    (51.3259405, -113.99610782),
    (51.33102662, -113.87401274),
    (51.2546757, -113.65191597),
    (51.11035407, -113.52633246),
    (51.01681467, -113.46005227),
    (50.84018427, -113.66005564),
    (50.72181249, -113.77401105),
])

# Load the configuration file
with open('config.json', 'r') as f:
    config = json.load(f)

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK']
AWS_ACCESS_KEY_ID = os.environ.get('AWS_DB_KEY', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_DB_SECRET_ACCESS_KEY', None)

discordUsername = "AB511"
discordAvatarURL = "https://pbs.twimg.com/profile_images/1256233970905341959/EKlyRkOM_400x400.jpg"

# Fallback mechanism for credentials
try:
    # Use environment variables if they exist
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name='us-east-1',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    else:
        # Otherwise, use IAM role permissions (default behavior of boto3)
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
except (NoCredentialsError, PartialCredentialsError):
    print("AWS credentials are not properly configured. Ensure IAM role or environment variables are set.")
    raise

# Specify the name of your DynamoDB table
table = dynamodb.Table(config['db_name'])

utc_timestamp = None

def update_utc_timestamp():
    global utc_timestamp
    utc_timestamp = calendar.timegm(datetime.now(timezone.utc).timetuple())

# set the current UTC timestamp for use in a few places
update_utc_timestamp()


# Function to convert the float values in the event data to Decimal, as DynamoDB doesn't support float type
def float_to_decimal(event):
    for key, value in event.items():
        if isinstance(value, float):
            event[key] = Decimal(str(value))
        elif isinstance(value, dict):
            event[key] = float_to_decimal(value)
    return event

def check_which_polygon_point(point):
    # Function to see which polygon a point is in, and returns the text. Returns "Other" if unknown.
    try:
        if polygon_Edmonton.contains(point):
            return 'Edmonton'
        elif polygon_Calgary.contains(point):
            return 'Calgary'
        elif polygon_NorthOfEdmonton.contains(point):
            return 'NorthOfEdmonton'
        elif polygon_SouthOfEdmonton.contains(point):
            return 'SouthOfEdmonton'
        else:
            return 'Other'
    except:
        return 'Other'

def getThreadID(threadName):
    if threadName == 'Edmonton':
        return config['Thread-Edmonton']
    elif threadName == 'Calgary':
        return config['Thread-Calgary']
    elif threadName == 'NorthOfEdmonton':
        return config['Thread-NorthOfEdmonton']
    elif threadName == 'SouthOfEdmonton':
        return config['Thread-SouthOfEdmonton']
    else:
        return config['Thread-CatchAll'] #Other catch all thread

def unix_to_readable(unix_timestamp):
    utc_time = datetime.fromtimestamp(int(unix_timestamp), timezone.utc)
    local_tz = pytz_timezone(config['timezone'])
    local_time = utc_time.astimezone(local_tz)
    return local_time.strftime('%Y-%b-%d %I:%M %p')

def post_to_discord_closure(event,threadName=None):
    # Create a webhook instance
    threadID = getThreadID(threadName)
    if threadID is not None:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL, thread_id=threadID)
    else:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL)

    #define type for URL
    if event['EventType'] == 'closures':
        URLType = 'Closures'
    elif event['EventType'] == 'accidentsAndIncidents':
        URLType = 'Incidents'
    else:
        URLType = 'Closures'


    urlWME = f"https://www.waze.com/en-GB/editor?env=usa&lon={event['Longitude']}&lat={event['Latitude']}&zoomLevel=15"
    url511 = f"https://511.alberta.ca/map#{URLType}-{event['ID']}"
    urlLivemap = f"https://www.waze.com/live-map/directions?dir_first=no&latlng={event['Latitude']}%2C{event['Longitude']}&overlay=false&zoom=16"

    embed = DiscordEmbed(title=f"Closed", color=15548997)
    embed.add_embed_field(name="Road", value=event['RoadwayName'])
    embed.add_embed_field(name="Direction", value=event['DirectionOfTravel'])
    embed.add_embed_field(name="Information", value=event['Description'], inline=False)
    embed.add_embed_field(name="Start Time", value=unix_to_readable(event['StartDate']))
    if 'PlannedEndDate' in event and event['PlannedEndDate'] is not None:
        embed.add_embed_field(name="Planned End Time", value=unix_to_readable(event['PlannedEndDate']))
    embed.add_embed_field(name="Links", value=f"[511]({url511}) | [WME]({urlWME}) | [Livemap]({urlLivemap})", inline=False)
    embed.set_footer(text=config['license_notice'])
    embed.set_timestamp(datetime.fromtimestamp(int(event['StartDate']), timezone.utc))
    # Send the closure notification
    webhook.add_embed(embed)
    webhook.execute()

def post_to_discord_updated(event,threadName=None):
    # Function to post to discord that an event was updated (already previously reported)
    # Create a webhook instance
    threadID = getThreadID(threadName)
    if threadID is not None:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL, thread_id=threadID)
    else:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL)

    #define type for URL
    if event['EventType'] == 'closures':
        URLType = 'Closures'
    elif event['EventType'] == 'accidentsAndIncidents':
        URLType = 'Incidents'
    else:
        URLType = 'Closures'

    urlWME = f"https://www.waze.com/en-GB/editor?env=usa&lon={event['Longitude']}&lat={event['Latitude']}&zoomLevel=15"
    url511 = f"https://511.alberta.ca/map#{URLType}-{event['ID']}"
    urlLivemap = f"https://www.waze.com/live-map/directions?dir_first=no&latlng={event['Latitude']}%2C{event['Longitude']}&overlay=false&zoom=16"

    embed = DiscordEmbed(title=f"Closure Update", color='ff9a00')
    embed.add_embed_field(name="Road", value=event['RoadwayName'])
    embed.add_embed_field(name="Direction", value=event['DirectionOfTravel'])
    embed.add_embed_field(name="Information", value=event['Description'], inline=False)
    embed.add_embed_field(name="Start Time", value=unix_to_readable(event['StartDate']))
    if 'PlannedEndDate' in event and event['PlannedEndDate'] is not None:
        embed.add_embed_field(name="Planned End Time", value=unix_to_readable(event['PlannedEndDate']))
    if 'Comment' in event and event['Comment'] is not None:
        embed.add_embed_field(name="Comment", value=event['Comment'], inline=False)
    embed.add_embed_field(name="Links", value=f"[511]({url511}) | [WME]({urlWME}) | [Livemap]({urlLivemap})", inline=False)
    embed.set_footer(text=config['license_notice'])
    embed.set_timestamp(datetime.fromtimestamp(int(event['LastUpdated']), timezone.utc))

    # Send the closure notification
    webhook.add_embed(embed)
    webhook.execute()

def post_to_discord_completed(event,threadName=None):
    # Create a webhook instance
    threadID = getThreadID(threadName)
    if threadID is not None:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL, thread_id=threadID)
    else:
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username=discordUsername, avatar_url=discordAvatarURL)

    urlWME = f"https://www.waze.com/en-GB/editor?env=usa&lon={event['Longitude']}&lat={event['Latitude']}&zoomLevel=15"
    urlLivemap = f"https://www.waze.com/live-map/directions?dir_first=no&latlng={event['Latitude']}%2C{event['Longitude']}&overlay=false&zoom=16"

    if 'lastTouched' in event:
        lastTouched = int(event['lastTouched'])
    else:
        lastTouched = utc_timestamp

    embed = DiscordEmbed(title=f"Cleared", color='34e718')
    embed.add_embed_field(name="Road", value=event['RoadwayName'])
    embed.add_embed_field(name="Direction", value=event['DirectionOfTravel'])
    embed.add_embed_field(name="Information", value=event['Description'], inline=False)
    embed.add_embed_field(name="Start Time", value=unix_to_readable(event['StartDate']))
    embed.add_embed_field(name="Ended", value=unix_to_readable(lastTouched))
    embed.add_embed_field(name="Links", value=f"[WME]({urlWME}) | [Livemap]({urlLivemap})", inline=False)
    embed.set_footer(text=config['license_notice'])
    embed.set_timestamp(datetime.fromtimestamp(lastTouched, timezone.utc))

    # Send the closure notification
    webhook.add_embed(embed)
    webhook.execute()

def check_and_post_events():
    #check if we need to clean old events
    last_execution_day = get_last_execution_day()
    today = date.today().isoformat()
    if last_execution_day is None or last_execution_day < today:
        # Perform cleanup of old events
        cleanup_old_events()

        # Update last execution day to current date
        update_last_execution_day()

    # Perform API call to ON511 API
    response = requests.get("https://511.alberta.ca/api/v2/get/event")
    if not response.ok:
        raise Exception('Issue connecting to AB511 API')

    #use the response to close out anything recent
    close_recent_events(response)
    # Parse the response
    data = json.loads(response.text)

    # Iterate over the events
    for event in data:
        # Check if the event is a full closure
        if event['IsFullClosure']:
            # Create a point from the event's coordinates
            point = Point(event['Latitude'], event['Longitude'])
            # Try to get the event with the specified ID and isActive=1 from the DynamoDB table
            dbResponse = table.query(
                KeyConditionExpression=Key('EventID').eq(str(event['ID'])),
                FilterExpression=Attr('isActive').eq(1),
                ConsistentRead=True
            )
            #If the event is not in the DynamoDB table
            update_utc_timestamp()
            if not dbResponse['Items']:
                # Set the EventID key in the event data
                event['EventID'] = str(event['ID'])
                # Set the isActive attribute
                event['isActive'] = 1
                # set LastTouched
                event['lastTouched'] = utc_timestamp
                event['DetectedPolygon'] = check_which_polygon_point(point)
                # Convert float values in the event to Decimal
                event = float_to_decimal(event)
                # If the event is within the specified area and has not been posted before, post it to Discord
                post_to_discord_closure(event,event['DetectedPolygon'])
                # Add the event ID to the DynamoDB table
                table.put_item(Item=event)
            else:
                # We have seen this event before
                # First, let's see if it has a lastupdated time
                event = float_to_decimal(event)
                lastUpdated = dbResponse['Items'][0].get('LastUpdated')
                if lastUpdated != None:
                    # Now, see if the version we stored is different
                    if lastUpdated != event['LastUpdated']:
                        # Store the most recent updated time:
                        event['EventID'] = str(event['ID'])
                        event['isActive'] = 1
                        event['lastTouched'] = utc_timestamp
                        event['DetectedPolygon'] = check_which_polygon_point(point)
                        # It's different, so we should fire an update notification
                        post_to_discord_updated(event,event['DetectedPolygon'])
                        table.put_item(Item=event)
                # Get the lastTouched time
                lastTouched = dbResponse['Items'][0].get('lastTouched')
                if lastTouched is None:
                    logging.warning(f"EventID: {event['ID']} - Missing lastTouched. Setting it now.")
                    lastTouched_datetime = now
                else:
                    lastTouched_datetime = datetime.fromtimestamp(int(lastTouched))
                # store the current time now
                now = datetime.fromtimestamp(utc_timestamp)
                # Compute the difference in minutes between now and lastUpdated
                time_diff_min = (now - lastTouched_datetime).total_seconds() / 60
                # Compute the variability
                variability = random.uniform(-2, 2)  # random float between -2 and 2
                # Add variability to the time difference
                time_diff_min += variability
                # Log calculated time difference and variability
                logging.info(
                    f"EventID: {event['ID']}, TimeDiff: {time_diff_min:.2f} minutes (Variability: {variability:.2f}), LastTouched: {lastTouched_datetime}, Now: {now}"
                )
                # If time_diff_min > 5, then more than 5 minutes have passed (considering variability)
                if abs(time_diff_min) > 5:
                    logging.info(f"EventID: {event['ID']} - Updating lastTouched to {utc_timestamp}.")
                    response = table.update_item(
                        Key={'EventID': str(event['ID'])},
                        UpdateExpression="SET lastTouched = :val",
                        ExpressionAttributeValues={':val': utc_timestamp}
                    )
                    logging.info(f"Update response for EventID {event['ID']}: {response}")
                    logging.info(f"EventID: {event['ID']} - lastTouched updated successfully.")
                # else:
                #     logging.info(f"EventID: {event['ID']} - No update needed. TimeDiff: {time_diff_min:.2f}")

def close_recent_events(responseObject):
    #function uses the API response from ON511 to determine what we stored in the DB that can now be closed
    #if it finds a closure no longer listed in the response object, then it marks it closed and posts to discord
    data = json.loads(responseObject.text)

    # Create a set of active event IDs
    active_event_ids = {str(event['ID']) for event in data}

    # Get the list of event IDs in the table
    response = table.scan(
        FilterExpression=Attr('isActive').eq(1)
    )
    # Iterate over the items
    for item in response['Items']:
        markCompleted = False
        # If an item's ID is not in the set of active event IDs, mark it as closed
        if item['EventID'] not in active_event_ids:
            markCompleted = True
        else:
            # item exists, but now we need to check to see if it's no longer a full closure
            event = [x for x in data if x['ID']==item['EventID']]
            if event:
                if event[0]['IsFullClosure'] is False:
                    #now it's no longer a full closure - markt it as closed.
                    markCompleted = True
        # process relevant completions
        if markCompleted == True:
            # Convert float values in the item to Decimal
            item = float_to_decimal(item)
            # Remove the isActive attribute from the item
            table.update_item(
                Key={'EventID': str(item['EventID'])},
                UpdateExpression="SET isActive = :val",
                ExpressionAttributeValues={':val': 0}
            )
            # Notify about closure on Discord
            if 'DetectedPolygon' in item and item['DetectedPolygon'] is not None:
                post_to_discord_completed(item,item['DetectedPolygon'])
            else:
                post_to_discord_completed(item)

def cleanup_old_events():
    # Get the current time and subtract 5 days to get the cut-off time
    now = datetime.now()
    cutoff = now - timedelta(days=5)
    # Convert the cutoff time to Unix timestamp
    cutoff_unix = Decimal(str(cutoff.timestamp()))
    # Initialize the scan parameters
    scan_params = {
        'FilterExpression': Attr('LastUpdated').lt(cutoff_unix) & Attr('isActive').eq(0)
    }
    while True:
        # Perform the scan operation
        response = table.scan(**scan_params)
        # Iterate over the matching items and delete each one
        for item in response['Items']:
            table.delete_item(
                Key={
                    'EventID': str(item['EventID'])
                }
            )
        # If the scan returned a LastEvaluatedKey, continue the scan from where it left off
        if 'LastEvaluatedKey' in response:
            scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            # If no LastEvaluatedKey was returned, the scan has completed and we can break from the loop
            break

def get_last_execution_day():
    response = table.query(
        KeyConditionExpression=Key('EventID').eq('LastCleanup')
    )

    items = response.get('Items')
    if items:
        item = items[0]
        last_execution_day = item.get('LastExecutionDay')
        return last_execution_day

    return None

def update_last_execution_day():
    today = datetime.now().date().isoformat()
    table.put_item(
        Item={
            'EventID': 'LastCleanup',
            'LastExecutionDay': today
        }
    )

def generate_geojson():
    # Create a dictionary to store GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    # Define your polygons and their names
    polygons = {
        "Edmonton": polygon_Edmonton,
        "Calgary": polygon_Calgary
    }

    # Convert each polygon to GeoJSON format
    for name, polygon in polygons.items():
        feature = {
            "type": "Feature",
            "properties": {
                "name": name
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    list(map(lambda coord: [coord[1], coord[0]], polygon.exterior.coords))  # Convert (lat, lon) to [lon, lat]
                ]
            }
        }
        geojson["features"].append(feature)

    # Write GeoJSON to a file
    with open("polygons.geojson", "w") as f:
        json.dump(geojson, f, indent=2)

    print("GeoJSON saved as 'polygons.geojson'")

def lambda_handler(event, context):
    check_and_post_events()

if __name__ == "__main__":
    # Simulate the Lambda environment by passing an empty event and context
    event = {}
    context = None
    lambda_handler(event, context)
