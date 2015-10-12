#!/usr/bin/python
import sys

path = ''

if len(str(sys.argv[1])) > 0:
    path = str(sys.argv[1])
else:
    raise NameError('Missing a valid path!')

def export_nodes(path):
    tweets = sqlContext.read.json(path)
    # tweets.printSchema()
    tweets.registerTempTable("tweets")
    #
    # TODO: need a starttime and end time
    #
    # nodes CSV
    nodes = sqlContext.sql("SELECT user.id_str as uuid, user.screen_name as screen_name, user.name as name, user.lang as language, max(user.statuses_count) as statuses_count, max(user.friends_count) as friends_count, max(user.listed_count) as listed_count, max(user.favourites_count) as favourites_count, max(user.followers_count) as followers_count FROM tweets GROUP BY user.id_str, user.screen_name, user.name, user.lang")
    nodes_arr = nodes.collect()
    import csv, unicodedata
    headers = ['uuid', 'screen_name', 'name', 'lang', 'statuses', 'friends', 'listed', 'favs', 'followers']
    with open('nodes.csv', 'wb') as csvfile:
        wr = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        wr.writerow(headers)
        for row in nodes_arr:
            writable = []
            for i in row:
                if type(i) is int:
                    writable.append( i )
                else:
                    writable.append( i.encode('ascii', 'ignore') )
            wr.writerow(writable)
# done

export_nodes(path)
