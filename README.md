# Video Games Database Project

## Background
This project was developed as part of my Data Analysis training with DataScientest. The capstone project focused on analyzing video game sales using data scraped from the Metacritic website.

I was responsible for collecting, processing, and analyzing raw data, including user and critic reviews. The final output is a structured SQLite database created using SQLAlchemy, containing tables ready for querying.

The dataset includes:
- 3,002 games with various variables of interest.
- 16,482 critic reviews and 8,825 user reviews from random samples.
- Sentiment analysis scores for reviews, using TextBlob, Vader, and Hugging Face.

For a larger dataset containing over 100,000 user reviews, visit my [Kaggle dataset](https://www.kaggle.com/datasets/aaronrmartin/video-games-gameplay-reviews-sentiment-scores).

## Features

### Raw Files
- **completed_records_plus_sentiment_metrics.csv:** 3,002 games with 45 variables, including sentiment metrics.
- **one_row_one_review_critics.csv:** 16,482 critic reviews for 340 games, including critic scores, Metacritic scores, and platforms.
- **one_row_one_review_users.csv:** 8,825 user reviews for 307 games, including user scores, global user scores, and platforms.
- **title_platform_user_and_critics_sentiment.csv:** 367 games with sentiment breakdowns (positive, mixed, negative) for users and critics.

### Transformed Files
- **13 Tables** are generated, including:
  - **GameVars Table:** Global and regional sales data.
  - **Ratings Table:** Metascore and global user scores.
  - **Gameplay Table:** Completion, difficulty, and playtime metrics for each game.
  - **SentimentScores Table:** Sentiment scores from TextBlob, Vader, and Hugging Face.
  - **CompositeSentimentScores Table:** Weighted averages of sentiment scores.
  - **GamePlatformSentimentScores Table:** Sentiment percentages and counts for users and critics.

### Scripts
- **prepare_tables_for_database.py:** Processes raw files into tables.
- **create_database.py:** Populates the `video_games.db` SQLite database.

## Future Enhancements
- Add SQL queries to demonstrate database capabilities, such as:
  - Analyzing global sales trends by genre, platform, or publisher.
  - Comparing user and critic sentiment across different platforms.
  - Visualizing sentiment trends over time.


    
