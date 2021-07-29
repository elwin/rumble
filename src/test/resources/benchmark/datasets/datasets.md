# Reddit Dataset
The following page contains many different data-dumps from Reddit. There are dumps in various sizes, probably any of
them should work. We tested the RC_2009-05.bz2 dataset. Please write it to a file named reddit.json in this directory.

https://files.pushshift.io/reddit/comments/
https://files.pushshift.io/reddit/comments/RC_2007-11.bz2 (202M)
https://files.pushshift.io/reddit/comments/RC_2009-05.bz2 (680M)

There are some transformations one can do (requires `pv` and `jq` to be installed).
Integers and Doubles:
pv reddit.json | jq '.score *= 1.5' -c > reddit_2.json

Integers, Doubles and Strings:
pv reddit.json | jq '.score *= 1.5 | if .score % 2 == 0 then .score |= tostring else . end' -c > reddit_3.json

# Further Datasets
- [git-archive](https://polybox.ethz.ch/index.php/s/HVWlvJAXVkQ05cw)
- [students](https://polybox.ethz.ch/index.php/s/E5XabV3JaKnr27E)
- [movies](https://polybox.ethz.ch/index.php/s/SvDkZnwLc5WrdFN)
