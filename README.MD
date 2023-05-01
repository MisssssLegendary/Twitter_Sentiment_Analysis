# Twitter Sentiment Analysis
In today's digital age, social media platforms like Twitter have become a crucial source of information for businesses, researchers, and policymakers. Black Friday is a significant shopping event that takes place every year, and Twitter is a platform where people express their views and share their experiences about this event. Analyzing the sentiment of tweets during Black Friday can provide valuable insights into consumer behavior and preferences.

This project is a demonstration of how to perform sentiment analysis on tweets using Apache Spark on Databricks, and then store and visualize the results using Amazon S3, Athena, and Amazon QuickSight.

## **Overview**
The project consists of several steps:

1. **Data Collection**: We planned to use the Twitter API to collect tweets containing a certain keyword. However, due to technical reasons, the data collected in this project came from an S3 bucket.

2. **Data Cleaning and Preparation**: We clean and prepare the collected data to be used in the sentiment analysis model, and create sentiment using VADER(Valence Aware Dictionary for sEntiment Reasoning).

3. **Sentiment Analysis Model**: We use Apache Spark on Databricks to train a sentiment analysis model on the cleaned data.

4. **Model Evaluation**: We evaluate the performance of the trained model.

5. **Visualization**: We store the results of the sentiment analysis and predictions to Amazon S3, create tables using Athena, and visualize the data in Amazon QuickSight.

## **[Databricks](https://github.com/MisssssLegendary/Twitter_Sentiment_Analysis/blob/main/Big_Data_Project_Twitter_Sentiment_Analysis.ipynb)**
In Databricks, we processed the tweet data by creating sentiment with VADER, and cleaning and transforming it into a format suitable for machine learning. We used PySpark and the NLTK library for data processing, and trained a logistic regression model for sentiment analysis. 

## **[Amazon Athena](https://github.com/MisssssLegendary/Twitter_Sentiment_Analysis/blob/main/Athena_queries.sql)**
After cleaning and processing the data in Databricks, we stored the cleaned data and model predictions in our S3 bucket. Next, we used Amazon Athena to further clean and organize the data. Specifically, we focused on cleaning the location data in the tweets, which was in a messy and unstructured format.

Using Athena, we created tables for visualization in Amazon QuickSight, which allowed us to easily create interactive dashboards to display our analysis. 

## **[Quicksight](https://github.com/MisssssLegendary/Twitter_Sentiment_Analysis/blob/main/Quick_Sight_Dashboard.jpg)**
Quicksight is a business analytics tool that allows users to create interactive visualizations, dashboards, and reports.

After creating tables using Athena, we used Amazon QuickSight to visualize the data. Our dashboard consisted of the following visualizations:
* **Word Cloud**: A visual representation of the most common words in the tweets. The size of each word is proportional to its frequency in the data.
        
    One notable finding was the prevalence of crypto-related content among the tweets, indicating the potential impact of cryptocurrency on Black Friday consumer behavior.
* **Top 25 Locations**: A bar chart showing the top 25 locations from which the tweets were sent. This visualization can help businesses identify regions where Black Friday is most popular and tailor their marketing strategies accordingly.
**Top 30 Users with most followers by Sentiment**: A Sankey diagram that lists the top 30 users who have the most followers by sentiment. This visualization helps to identify influential users who may be driving the sentiment.
* **Count of Sentiment by Sentiment**: A pie chart that shows the distribution of sentiment in the data. This visualization provides a high-level overview of the overall sentiment of the tweets.
* **Count of Records by Prediction and Sentiment**: A table that shows the count of records by prediction and sentiment. This visualization helps to identify any discrepancies between the predicted sentiment and the actual sentiment of the tweets.
* **Top 30 Users by Tweeted Count by Sentiment**: A Sankey diagrams that lists the top 30 users who tweeted the most by sentiment. This visualization helps to identify users who may be driving the sentiment.

## **Conclusion**
By performing sentiment analysis on tweets related to a specific topic, we can gain insights into public opinion and trends. This project demonstrates how to use machine learning techniques and cloud services to process and analyze large amounts of tweet data, and to create visualizations that help to communicate the results.