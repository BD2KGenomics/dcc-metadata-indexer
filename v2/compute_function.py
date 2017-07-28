import boto3 as b
import pytz as p
from decimal import Decimal
import datetime as dt


def calculate_compute_cost(start_time, end_time, instance_type, region_name, instance_description='Linux/UNIX',
                           availability_zone='us-west-2b'):
    if (not instance_type):
        return ("No instance_type specified!")

    elif (not region_name):
        return ("No region_name specified!")

    elif (not start_time or not end_time):
        return ("Specify start_time and end_time!\nWith format yyyy-mm-ddTHH:MM:SS.MSS")
    else:
        try:
            # Set up time
            startDatetime = dt.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=p.UTC)
            endDatetime = dt.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=p.UTC) + dt.timedelta(
                hours=2)
            block = dt.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=p.UTC)

        except:
            s = start_time[:-6]
            e = end_time[:-6]
            startDatetime = dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=p.UTC)
            endDatetime = dt.datetime.strptime(e, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=p.UTC) + dt.timedelta(hours=2)
            block = dt.datetime.strptime(e, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=p.UTC)

        # Set up infrastructure for making calls to aws, with the desired parameters.
        client = b.client('ec2', region_name=region_name)
        response = client.describe_spot_price_history(AvailabilityZone=availability_zone, MaxResults=999,
                                                      InstanceTypes=[str(instance_type)], StartTime=startDatetime,
                                                      EndTime=endDatetime,
                                                      ProductDescriptions=[str(instance_description)])

        last_time_checked = response
        price = 0
        for spot_price in reversed(response['SpotPriceHistory']):

            if (spot_price['Timestamp'] < startDatetime):
                last_time_checked = spot_price

            elif ((spot_price['Timestamp'] >= startDatetime) and (spot_price['Timestamp'] < block)):
                if ((startDatetime <= spot_price['Timestamp']) and (startDatetime >= last_time_checked['Timestamp'])):
                    delta = startDatetime - last_time_checked['Timestamp']
                    minutes = float(delta.seconds) / float(60)
                    rate = (float(minutes) / float(60)) * float(last_time_checked['SpotPrice'])
                    price += Decimal(rate)
                    last_time_checked = spot_price
                elif ((startDatetime < last_time_checked['Timestamp']) and (
                    spot_price['Timestamp'] > last_time_checked['Timestamp'])):
                    delta = spot_price['Timestamp'] - last_time_checked['Timestamp']
                    minutes = float(delta.seconds) / float(60)
                    rate = (float(minutes) / float(60)) * float(last_time_checked['SpotPrice'])
                    price += Decimal(rate)
                    last_time_checked = spot_price
            elif ((last_time_checked['Timestamp'] <= block) and (spot_price['Timestamp'] > block)):
                delta = block - last_time_checked['Timestamp']
                minutes = float(delta.seconds) / float(60)
                rate = (float(minutes) / float(60)) * float(last_time_checked['SpotPrice'])
                price += Decimal(rate)
                last_time_checked = spot_price

        return price
