Q1:
We first group the dataset by an hourly window that is updated every 30 minutes, and do a wordcount.
The output was implemented with ForeachWriter. (This could also be done with 'numRows' option, but we used an approach which can be used for Q3 as well)

Q2:
We used trigger(ProcessingTime("10 seconds")) to control the time window. To get the twitter IDs of users that have been mentioned by other users, we used select("user2").where("interaction='MT'").
Because we are not using aggregation for this problem, so we can safely output the results to parquet file.

Q3:
We used trigger(ProcessingTime("5 seconds")) to output the results every 5 seconds. For each window, we first select the users who initiated the event (i.e. user in the first column) and appeared in our given list.
Then do a word count.
The output was accomplished with a ForeachWriter.
