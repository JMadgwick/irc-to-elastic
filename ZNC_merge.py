import os,locale,argparse
from datetime import datetime

parser = argparse.ArgumentParser(description='Simple utility to merge a directory of ZNC IRC chat log files into a single file. By default, ISO format is used for the timestamp (eg. "2011-02-13T01:55:34").')
parser.add_argument('directory', help='Path to directory containing ZNC log files')
parser.add_argument('-l', '--local', action='store_true', help='Use local locale based timestamp instead of ISO format')
parser.add_argument('output', type=argparse.FileType('w'), help='File to write output into')
args = parser.parse_args()

if args.local:
    #Set locale to local locale
    locale.setlocale(locale.LC_ALL, '')
    dateTimeFormat = '%c'
else:
    dateTimeFormat = '%Y-%m-%dT%H:%M:%S'

files = os.listdir(args.directory)
#Filter out non log files and remove extension to allow easy sorting
logs = [fn[:-4] for fn in files if fn[-4:] == '.log']
#Sort logs into date order
logs.sort()

for log in logs:
    #Open file and read log
    logFile = open(args.directory + '/' + log + '.log', 'r')
    logText = logFile.read().splitlines()
    logFile.close()
    #Use filename as date and merge with time from the log
    for line in logText:
        args.output.write(datetime.strptime(log[:10] + line[1:9],'%Y-%m-%d%H:%M:%S').strftime(dateTimeFormat) + ' ' + line[11:] + '\n')

args.output.close()
