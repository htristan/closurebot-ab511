import requests
import json
import time
import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.conditions import Key
from shapely.geometry import Point, Polygon
from decimal import Decimal
from discord_webhook import DiscordWebhook, DiscordEmbed
import os
from datetime import datetime, timedelta, date
import calendar
from pytz import timezone
import random

# Define the coordinates of your polygon

polygon_Edmonton = Polygon([
    (53.145412629840, -113.110338310830),
    (53.133618023846, -113.340657872860),
    (53.141650208939, -113.415363179675),
    (53.185309395177, -113.550852018988),
    (53.250021793499, -114.015719162410),
    (53.331991137832, -114.015270072968),
    (53.405807981906, -114.003186462043),
    (53.513507814880, -113.503457560441),
    (53.523333536896, -113.321879725283),
    (53.523863036374, -113.172510917277),
    (53.465937518909, -113.041471167407),
    (53.343459108593, -113.005262140255),
    (53.145412629840, -113.110338310830)
])

polygon_NorthOfEdmonton = Polygon([
    (53.300876021229, -110.001358362146),
    (53.343459108593, -113.005262140255),
    (53.465937518909, -113.041471167407),
    (53.523863036374, -113.172510917277),
    (53.523333536896, -113.321879725283),
    (53.513507814880, -113.503457560441),
    (53.405807981906, -114.003186462043),
    (53.331991137832, -114.015270072968),
    (53.355050218684, -119.542258413305),
    (53.510387473939, -120.010437403490),
    (54.573962347602, -119.591363798759),
    (59.595643462352, -119.571817815078),
    (60.013329523244, -109.584164455951),
    (54.341313857318, -109.584164455951),
    (54.024679701645, -110.004363657639),
    (53.300876021229, -110.001358362146)
])

polygon_SouthOfEdmonton = Polygon([
    (53.331991137832, -114.015270072968),
    (53.250021793499, -114.015719162410),
    (53.185309395177, -113.550852018988),
    (53.141650208939, -113.415363179675),
    (53.133618023846, -113.340657872860),
    (53.145412629840, -113.110338310830),
    (53.343459108593, -113.005262140255),
    (53.300876021229, -110.001358362146),
    (53.190343405643, -110.000345969085),
    (53.131088046805, -110.000755241349),
    (53.085104606671, -110.000755241349),
    (52.503884092304, -110.001214074637),
    (48.595686997533, -110.000974039367),
    (48.594849342667, -111.415637903059),
    (48.595494605321, -113.291601114147),
    (49.001677876848, -114.040893256449),
    (49.030649801787, -114.030237322733),
    (49.044583033539, -114.073970380421),
    (49.082610258229, -114.095282248176),
    (49.093140583204, -114.095282248176),
    (49.112985807060, -114.140426887233),
    (49.103185048627, -114.170915592251),
    (49.124717213677, -114.235960517694),
    (49.144549423574, -114.230044132205),
    (49.155307152399, -114.264970126582),
    (49.231891862679, -114.342082567110),
    (49.271223904432, -114.364503757043),
    (49.304963866870, -114.345198585100),
    (49.340040803061, -114.350519856469),
    (49.331723491681, -114.381748109714),
    (49.333402572078, -114.440506875393),
    (49.371455584739, -114.443465068299),
    (49.384556356747, -114.394252914158),
    (49.422331112652, -114.393513365931),
    (49.442761672542, -114.373310820354),
    (49.461034111786, -114.391664495526),
    (49.581009857221, -114.402690203194),
    (50.231143008950, -114.485349255583),
    (50.342674945711, -115.013892494932),
    (50.314228453452, -115.122233188704),
    (50.431196665914, -115.214808626582),
    (50.435187475059, -115.260272093148),
    (50.510007266273, -115.380636063290),
    (50.542713089542, -115.345282908361),
    (51.004872102267, -115.403782010725),
    (51.050660574798, -115.473420118113),
    (51.040569852886, -116.040180691249),
    (51.224656809434, -116.221784910466),
    (51.243467288588, -116.190828456848),
    (52.164867101727, -117.445759051765),
    (52.460041914206, -118.272451961568),
    (52.561896348174, -118.361051584206),
    (53.150937136610, -118.594240045023),
    (53.132996119532, -119.205586499763),
    (53.310703307314, -119.521837780852),
    (53.355050218684, -119.542258413305),
    (53.331991137832, -114.015270072968),
    (51.125267404285, -114.313067141666),
    (51.041003382040, -114.365300243223),
    (50.530319688236, -114.364044407942),
    (50.441946793046, -114.281392391303),
    (50.423876768221, -114.005715213603),
    (50.431852496297, -113.462643978018),
    (50.502466338085, -113.393620030788),
    (51.010053280366, -113.273618816959),
    (51.063727465943, -113.313479684387),
    (51.151683251112, -113.390689748682),
    (51.195169581680, -113.522644585095),
    (51.193338581422, -113.594598814432),
    (51.180965708581, -114.162228400886),
    (51.125267404285, -114.313067141666),
])


polygon_Calgary = Polygon([
    (50.431852496297, -113.462643978018),
    (50.423876768221, -114.005715213603),
    (50.441946793046, -114.281392391303),
    (50.530319688236, -114.364044407942),
    (51.041003382040, -114.365300243223),
    (51.125267404285, -114.313067141666),
    (51.180965708581, -114.162228400886),
    (51.193338581422, -113.594598814432),
    (51.195169581680, -113.522644585095),
    (51.151683251112, -113.390689748682),
    (51.063727465943, -113.313479684387),
    (51.010053280366, -113.273618816959),
    (50.502466338085, -113.393620030788),
    (50.431852496297, -113.462643978018)
])

# Load the configuration file
with open('config.json', 'r') as f:
    config = json.load(f)

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK']
AWS_ACCESS_KEY_ID = os.environ['AWS_DB_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_DB_SECRET_ACCESS_KEY']

discordUsername = "AB511"
discordAvatarURL = "https://pbs.twimg.com/profile_images/1256233970905341959/EKlyRkOM_400x400.jpg"

# Create a DynamoDB resource object
dynamodb = boto3.resource('dynamodb',
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

# Specify the name of your DynamoDB table
table = dynamodb.Table(config['db_name'])

# set the current UTC timestamp for use in a few places
utc_timestamp = calendar.timegm(datetime.utcnow().timetuple())

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
    utc_time = datetime.utcfromtimestamp(int(unix_timestamp))
    local_tz = timezone(config['timezone'])
    local_time = utc_time.replace(tzinfo=timezone('UTC')).astimezone(local_tz)
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
    embed.set_timestamp(datetime.utcfromtimestamp(int(event['StartDate'])))
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
    embed.set_timestamp(datetime.utcfromtimestamp(int(event['LastUpdated'])))

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
    embed.set_timestamp(datetime.utcfromtimestamp(lastTouched))

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
                KeyConditionExpression=Key('EventID').eq(event['ID']),
                FilterExpression=Attr('isActive').eq(1)
            )
            #If the event is not in the DynamoDB table
            if not dbResponse['Items']:
                # Set the EventID key in the event data
                event['EventID'] = event['ID']
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
                        event['EventID'] = event['ID']
                        event['isActive'] = 1
                        event['lastTouched'] = utc_timestamp
                        event['DetectedPolygon'] = check_which_polygon_point(point)
                        # It's different, so we should fire an update notification
                        post_to_discord_updated(event,event['DetectedPolygon'])
                        table.put_item(Item=event)
                # get the lasttouched time
                lastTouched_datetime = datetime.fromtimestamp(int(dbResponse['Items'][0].get('lastTouched')))
                # store the current time now
                now = datetime.fromtimestamp(utc_timestamp)
                # Compute the difference in minutes between now and lastUpdated
                time_diff_min = (now - lastTouched_datetime).total_seconds() / 60
                # Compute the variability
                variability = random.uniform(-2, 2)  # random float between -2 and 2
                # Add variability to the time difference
                time_diff_min += variability
                # If time_diff_min > 5, then more than 5 minutes have passed (considering variability)
                if abs(time_diff_min) > 5:
                    # let's store that we just saw it to keep track of the last touch time
                    table.update_item(
                        Key={'EventID': event['ID']},
                        UpdateExpression="SET lastTouched = :val",
                        ExpressionAttributeValues={':val': utc_timestamp}
                    )

def close_recent_events(responseObject):
    #function uses the API response from ON511 to determine what we stored in the DB that can now be closed
    #if it finds a closure no longer listed in the response object, then it marks it closed and posts to discord
    data = json.loads(responseObject.text)

    # Create a set of active event IDs
    active_event_ids = {event['ID'] for event in data}

    # Get the list of event IDs in the table
    response = table.scan(
        FilterExpression=Attr('isActive').eq(1)
    )

    # Iterate over the items
    for item in response['Items']:
        # If an item's ID is not in the set of active event IDs, mark it as closed
        if item['EventID'] not in active_event_ids:
            # Convert float values in the item to Decimal
            item = float_to_decimal(item)
            # Remove the isActive attribute from the item
            table.update_item(
                Key={'EventID': item['EventID']},
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
                    'EventID': item['EventID']
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

def lambda_handler(event, context):
    check_and_post_events()