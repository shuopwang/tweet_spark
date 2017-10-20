# tweet_spark

You can see the result at https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3720970161315096/2844018759966085/6059703217653741/latest.html

Using the spark to analyze the Tweets which are in the format of JSON, based on databricks.  

Using the RDD to find all the tags of tweets and show the top 10 tags in those tweets.  

Find the top 10 users who send more tweets.  

FINDING the trending topic :) The strategy is following, count all the tags per day, and then just show the tags whose count number is greater than 20.  

It is written by scala.
