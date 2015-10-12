#!/usr/bin/python
import sys

path = ''

if len(str(sys.argv[1])) > 0:
    path = str(sys.argv[1])
else:
    raise NameError('Missing a valid path!')

# takes first argument as path

def export_edges(path):
    tweets = sqlContext.read.json(path)
    #tweets.printSchema()
    tweets.registerTempTable("tweets")
    #
    # TODO: need a name for the edge
    #
    import dateutil.parser
    def utcoffset():
        return 0
    # for condor, timestamps should be in the format of 2015-09-21T09:15:13.000+03:00
    # time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d
    # time.strptime('Thu Sep 24 19:52:07 +0000 2015').isoformat()
    # import dateutil.parser
    # dateutil.parser.parse('Thu Sep 24 19:52:07 +0000 2015').isoformat()
    # edges CSV
    mentions = sqlContext.sql("SELECT user.id_str as source_id, entities.user_mentions.id_str as mentions, text, created_at as timestamp FROM tweets WHERE entities.user_mentions IS NOT NULL")
    mentions_bulk_arr = mentions.collect()
    import unicodedata
    # the output array with the format [source_id, target_id, text, connection_type]
    ment_disaggr_arr = []
    # construct "ment_disaggr_arr" output array, strip weird unicode characters from text
    for ment_row in mentions_bulk_arr:
        for ment in ment_row.mentions:
            ment_disaggr_arr.append([str(ment_row.source_id), str(ment), str(ment_row.text.encode('ascii', 'ignore').rstrip('\n')), str(dateutil.parser.parse(ment_row.timestamp).isoformat()), 'mention'])
    #done
    retweets = sqlContext.sql("SELECT user.id_str as uuid, retweeted_status.user.id_str as target_id, text, created_at as timestamp FROM tweets WHERE retweeted_status.user.id_str IS NOT NULL")
    retweets_arr = retweets.collect()
    # initialize "to_csv_arr" and add the processed mentions array; format [source_id, target_id, text, connection_type]
    edges_to_csv_arr = []
    edges_to_csv_arr = edges_to_csv_arr + ment_disaggr_arr
    # add the retweets in the correct format; format [source_id, target_id, text, connection_type]
    for retweet in retweets_arr:
        edges_to_csv_arr.append([retweet.uuid, retweet.target_id, str(retweet.text.encode('ascii', 'ignore').rstrip('\n')), str(dateutil.parser.parse(retweet.timestamp).isoformat()), 'retweet'])
    # start generating the edge CSV file
    import csv
    headers = ['source', 'target', 'text', 'timestamp', 'connection_type']
    # write the actual file
    with open('edges.csv', 'wb') as csvfile:
        wr = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        wr.writerow(headers)
        for row in edges_to_csv_arr:
            wr.writerow(row)
            #done
        #done
    #done
#done

export_edges(path)
# final
