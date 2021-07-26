FILENAME="RC_2009-05.bz2"

echo "downloading..."
curl "https://files.pushshift.io/reddit/comments/${FILENAME}" -o ${FILENAME}

echo "unzipping..."
pv ${FILENAME} | bunzip2 > reddit.json

echo "generating reddit_2.json"
pv reddit.json | jq '.score *= 1.5' -c > reddit_2.json

echo "generating reddit_3.json"
pv reddit.json | jq '.score *= 1.5 | if .score % 2 == 0 then .score |= tostring else . end' -c > reddit_3.json