import os,re,json,argparse
from datetime import datetime
from elasticsearch import Elasticsearch

parser = argparse.ArgumentParser(description='Utility to parse a merged ZNC IRC chat log (or a logbot archive) file and index it into a local Elasticsearch. Other log types (e.g. echelog) can usually be parsed if they consist of chat messages only.')
parser.add_argument('input', type=argparse.FileType('r'), help='File to read from')
parser.add_argument('-t', '--inputType', choices=['ZNC', 'logbot'], default='ZNC', help='Type of log, Merged ZNC log (default) or logbot archive')
parser.add_argument('-i', '--index', default='irc', help="Name of Elasticsearch index to use (default 'irc')")
args = parser.parse_args()


es = Elasticsearch()

inputDatetimeFormat = '%Y-%m-%dT%H:%M:%S'

evJoinRe = re.compile(r'\*\*\* Joins: (\S+) \((\S+)\)')
evQuitRe = re.compile(r'\*\*\* Quits: (\S+) \((\S+)\) \((.*)\)')
evPartRe = re.compile(r'\*\*\* Parts: (\S+) \((\S+)\) \((.*)\)')
evModeRe = re.compile(r'\*\*\* (\S+) sets mode: (\S+) ?(\S+)?')
evNickRe = re.compile(r'\*\*\* (\S+) is now known as (\S+)')
emoteRe = re.compile(r'\* (\S+) (.*)')
chatRe = re.compile(r'<(\S+)> ?(.*)')#It is possible to have a message with no content!
serverRe = re.compile(r'-(\S+)- ?(.*)')
logbotRe = re.compile(r'#\S+ (.+)')

#Open file and read log
logText = args.input.read().split('\n')#splitlines() can't be used as it interpets certain unicode chars which could in messages as newline - https://docs.python.org/3/library/stdtypes.html#str.splitlines
args.input.close()

bulkBody = ''
lineNo = 0

for line in logText:
    lineNo += 1
    if line == '':
        continue
    if args.inputType == 'logbot':
        docDict = {'date' : datetime.strptime(line[:19],inputDatetimeFormat).strftime('%Y-%m-%dT%H:%M:%S')}
        line = logbotRe.match(line[20:])[1]
    elif args.inputType == 'ZNC':
        docDict = {'date' : datetime.strptime(line[:19],inputDatetimeFormat).strftime('%Y-%m-%dT%H:%M:%S')}
        line = line[20:]
    if match := chatRe.match(line):
        docDict['user'] = match[1]
        docDict['msg'] = match[2]
    elif match := evJoinRe.match(line):
        docDict['join'] = match[1]
        docDict['info'] = match[2]
    elif match := evQuitRe.match(line):
        docDict['quit'] = match[1]
        docDict['info'] = match[2]
        if match[3] != '': #Don't index when empty
            docDict['reason'] = match[3]
    elif match := evNickRe.match(line):
        docDict['oldnick'] = match[1]
        docDict['newnick'] = match[2]
    elif match := evModeRe.match(line):
        docDict['source'] = match[1]
        docDict['modes'] = match[2]
        if match.group(3): #Not always present
            docDict['target'] = match[3]
    elif match := evPartRe.match(line):
        docDict['part'] = match[1]
        docDict['info'] = match[2]
        if match[3] != '': #Don't index when empty
            docDict['extra'] = match[3]
    elif match := emoteRe.match(line):
        docDict['user'] = match[1]
        docDict['emote'] = match[2]
    elif match := serverRe.match(line):
        docDict['server'] = match[1]
        docDict['msg'] = match[2]
    else:
        print("ERROR - Couldn't parse line " + str(lineNo) + ': ' + line)
        exit()
    bulkBody += '{ "index" : {} }' + '\n' + json.dumps(docDict) + '\n'

result = es.bulk(body=bulkBody, index=args.index)
print('Elastic Bulk Index finished, time (ms): ' + str(result['took']) + ', errors: ' + str(result['errors']) + ', total documents indexed: ' + str(len(result['items'])))
