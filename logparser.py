#!/usr/bin/python
import sys
import argparse
import csv
import urlparse
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from io import BytesIO
from gzip import GzipFile


# Try import of boto3.
# https://aws.amazon.com/sdk-for-python/
try:
    import boto3
    #import botocore
except Exception as e:
    print('ERROR: Unable to import boto3 - '+str(e.message))
    print """
        You must install boto3 for this script to work
        # pip install boto3
        """
    sys.exit(1)



# Verify that the S3 bucket exists
bucket = 'techtest-alb-logs'
try:
    s3client = boto3.client('s3')
    s3resource = boto3.resource('s3')
    s3resource.meta.client.head_bucket(Bucket=bucket)
    s3logbucket = s3resource.Bucket(bucket)
except Exception as e:
    print('ERROR: Unable to access bucket'+str(bucket)+" - "+str(e.message))
    sys.exit(1)



# Valid Arguements
validreports = ('getcodes', 'geturls', 'getUAs', 'getreport')
validunits = ('years', 'months', 'days', 'hours', 'minutes')


# Get the command line Arguements and parse them out
parser = argparse.ArgumentParser(description='Load balancer logs parser from S3 bucket' )
parser.add_argument('report', nargs=1, help='the type of report to return VALID '+str(validreports))
parser.add_argument('--code', type=int, nargs=1, help='error code (ex 404)')
parser.add_argument('--from', nargs=1, help='date range (ex --from 2017/10/13)')
parser.add_argument('--to', nargs=1,  help='date range (ex --to 2017/10/20)')
parser.add_argument('--for', nargs=2, metavar=('value', 'unit'), help='specify a relative time (ex 7 days) VALID '+str(validunits))
parser.add_argument('--max', nargs=1, type=int, help='Max number of records to return.', default=-1)
args = vars(parser.parse_args())
#Uncomment to troubleshoot args getting passed in
#print "ARGS:" +str(args)


# Check each argument
####################################
# Chech the report
if not args['report'][0] in validreports:
    parser.error("report must be set to " +str(validreports))

# Make sure absolute or relative data options
if (args['from'] is None or args['to'] is None) and (args['for'] is None):
    parser.error ("Must specify [--from date --to date ] | [--for value unit]")

# check if for absolute dates
if not (args['from'] is None or args['to'] is None):
    fromdate = datetime.strptime(args['from'][0]+" 00:00:00", '%Y/%m/%d %H:%M:%S')
    todate = datetime.strptime(args['to'][0]+" 23:59:59", '%Y/%m/%d  %H:%M:%S')

#  check for relative dates
if not (args['for'] is None):
    relativeval = int(args['for'][0])
    relativeunit = str(args['for'][1])
    if relativeunit not in validunits:
        parser.error("The unit must be one of the following: "+str(validunits))
    todate = datetime.now()
    fromdate = todate - relativedelta(**{relativeunit : relativeval})


# Create a list of days for us to read from S3.  The Year/Month/Day will be part of the path.
days = [fromdate + timedelta(days=x) for x in range((todate-fromdate).days + 1)]

#uncomment to troubleshoot
#print "FROM: "+str(fromdate)
#print "TO: "+str(todate)
#print "DAYS: "+str(days)


# Main loop to Fetch the logs for the specified days
count = 0
for day in days:
    # Pull the objects from S3 for the specified datedirectory path
    datedirectory = "webservices/AWSLogs/158469572311/elasticloadbalancing/us-west-2/"+day.strftime('%Y/%m/%d')
    for s3obj in s3logbucket.objects.filter(Prefix=datedirectory):

        # for each object read unzip and read the file
        try:
            obj = s3client.get_object(Bucket=bucket, Key=s3obj.key)
            bytestream = BytesIO(obj['Body'].read())
            got_text = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        except:
            raise

        # Loop through each line and parse out the info we need
        for log_line in csv.reader(got_text.splitlines(), delimiter = ' '):
            logentry = {}
            logentry['timestamp'] = datetime.strptime('%s %s' % (log_line[1].split("T")[0],log_line[1].split("T")[1].split(".")[0]), '%Y-%m-%d %H:%M:%S')

            # Check to see if the timestamp is valid for report.  If not then it will not be worth parsing anything else out
            if fromdate <= logentry['timestamp'] <= todate:
                try:
                    logentry['type'] = str(log_line[0])
                    logentry['elbname'] = str(log_line[2])
                    logentry['clientport'] = str(log_line[3])
                    logentry['targetport'] = str(log_line[4])
                    logentry['request_proc_time'] = float(log_line[5])
                    logentry['target_proc_time'] = float(log_line[6])
                    logentry['response_proc_time'] = float(log_line[7])
                    logentry['elb_status_code'] = log_line[8]
                    logentry['target_status_code'] = log_line[9]
                    logentry['received_bytes'] = int(log_line[10])
                    logentry['sent_bytes'] = int(log_line[11])
                    logentry['request'] = log_line[12]
                    logentry['method'] = log_line[12].split(" ")[0]
                    url = urlparse.urlsplit(log_line[12].split(" ")[1])
                    logentry['request_protocol'] = url.scheme
                    logentry['request_host'] = url.hostname
                    logentry['path'] = url.path
                    logentry['querystring'] = url.query
                    logentry['user_agent'] = log_line[13]
                    logentry['ssl_cipher'] = log_line[14]
                    logentry['ssl_protocol'] = log_line[15]
                    logentry['target_group_arn'] = log_line[16]
                    logentry['trace_id'] = log_line[17]
                except Exception as e:
                    print "ERROR parsing line - "+str(log_line)
                    raise

                # getcodes report
                if args['report'][0] == 'getcodes':
                    if int(logentry['elb_status_code']) >= 400:
                        print str(logentry['elb_status_code']) +" - " +str(logentry['target_status_code']) +" - " +str(logentry['timestamp']) +" - "+ logentry['request']
                        count = count + 1

                # if outputed more than max we are all done.  No need to do more
                if args['max'][0] != -1 and count >= args['max'][0]:
                    sys.exit(0)
