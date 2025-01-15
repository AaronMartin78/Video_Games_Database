import pandas as pd
import os 
import numpy as np

#os.chdir(r'Raw Files')
os.chdir(r'D:\GitHub_Files\Video_Games_Database_Files')

# Create GameID
df = pd.read_csv(r'Raw Files\completed_records_plus_sentiment_metrics.csv')

games = df[['game_title']].drop_duplicates().reset_index(drop=True)
games['GameID'] = games.index + 1

# Set explicit data types
games['GameID'] = games['GameID'].astype(int)  
games['game_title'] = games['game_title'].astype(str)  

# Fill missing game titles with a placeholder or drop them
games['game_title'] = games['game_title'].fillna('Unknown')

games.set_index('GameID', inplace=True)

# Save to a new CSV
games.to_csv(r'Transformed Files\GameID.csv')
print(games.head())
print(games.info())



# Creat PlatformsID
df = pd.read_csv(r'Raw Files\title_platform_user_and_critics_sentiment.csv')

platforms = df[['platform']].drop_duplicates().reset_index(drop=True)
platforms = platforms.sort_values(by='platform', ascending=True)
platforms.reset_index(drop=True, inplace=True)
platforms['PlatformID'] = platforms.index + 1

#  Set explicit data types
platforms['PlatformID'] = platforms['PlatformID'].astype(int) 
platforms['platform'] = platforms['platform'].astype(str)  

# Fill missing game titles with a placeholder or drop them
platforms['platform'] = platforms['platform'].fillna('Unknown')

platforms.set_index('PlatformID', inplace=True)

# Save to a new CSV
platforms.to_csv(r'Transformed Files\PlatformID.csv')
print(platforms.head())



# Create Game-Platform Composite ID
df = pd.read_csv(r'Raw Files\title_platform_user_and_critics_sentiment.csv')

# Define merge docs
GamesID = pd.read_csv(r'Transformed Files\GameID.csv') 
PlatformsID = pd.read_csv(r'Transformed Files\PlatformID.csv')

game_platform_composite = df[['game_title', 'platform']].drop_duplicates().reset_index(drop=True)

# Sort by platform in ascending order
game_platform_composite = game_platform_composite.sort_values(by='platform', ascending=True)

# Reset the index after sorting to ensure sequential indices
game_platform_composite.reset_index(drop=True, inplace=True)

# Merge CriticID, GameID, and PlatformID with the raw data
game_platform_composite = game_platform_composite.merge(GamesID, on='game_title').merge(PlatformsID, on='platform')

# Create the composite ID (this will now be based on the sorted DataFrame)
game_platform_composite['GamePlatformID'] = game_platform_composite.index + 1  

# Keep only the relevant columns
game_platform_composite = game_platform_composite[['GamePlatformID', 'GameID', 'PlatformID', 'game_title', 'platform']]

# Set explicit data types
game_platform_composite['GamePlatformID'] = game_platform_composite['GamePlatformID'].astype(int)  
game_platform_composite['GameID'] = game_platform_composite['GameID'].astype(int) 
game_platform_composite['PlatformID'] = game_platform_composite['PlatformID'].astype(int)

game_platform_composite.set_index('GamePlatformID', inplace=True)

# Save it to a CSV
game_platform_composite.to_csv(r'Transformed Files\GamePlatformID.csv')
print(game_platform_composite.head())



# Create CriticsID
df = pd.read_csv(r'Raw Files\one_row_one_review_critics.csv')  

# Create a unique ID for each critic
critics = df[['critic_name']].drop_duplicates().reset_index(drop=True)
critics = critics.sort_values(by='critic_name', ascending=True)
critics.reset_index(drop=True, inplace=True)
critics['CriticID'] = critics.index + 1  

# Set explicit data types
critics['CriticID'] = critics['CriticID'].astype(int)  
critics['critic_name'] = critics['critic_name'].astype(str)  

# Fill missing game titles with a placeholder or drop them
critics['critic_name'] = critics['critic_name'].fillna('Unknown')

critics.set_index('CriticID', inplace=True)

# Save to a new CSV
critics.to_csv(r'Transformed Files\CriticID.csv')
print(critics.head())



# Create UsersID
df = pd.read_csv(r'Raw Files\one_row_one_review_users.csv')  
df.head(5)
users = df[['username']].drop_duplicates().reset_index(drop=True)
users = users.sort_values(by='username', ascending=True)
users.reset_index(drop=True, inplace=True)
users['UserID'] = users.index + 1 
users.head()

# Set explicit data types
users['UserID'] = users['UserID'].astype(int)  
users['username'] = users['username'].astype(str)  

# Fill missing game titles with a placeholder or drop them
users['username'] = users['username'].fillna('Unknown')

users.set_index('UserID', inplace=True)

# Save to a new CSV
users.to_csv(r'Transformed Files\UserID.csv')
print(users.head())



# Critics Review Table
df = pd.read_csv(r'Raw Files\one_row_one_review_critics.csv')

# Define CriticsID, GamesID, and PlatformsID DataFrames
CriticsID = pd.read_csv(r'Transformed Files\CriticID.csv') 
GamesID = pd.read_csv(r'Transformed Files\GameID.csv') 
PlatformsID = pd.read_csv(r'Transformed Files\PlatformID.csv') 

# Merge CriticID, GameID, and PlatformID with the raw data
critic_reviews = df.merge(CriticsID, on='critic_name').merge(GamesID, on='game_title').merge(PlatformsID, on='platform')

# Create Identifier
critic_reviews['CriticReviewID'] = critic_reviews.index + 1 

# Keep only the relevant columns
critic_reviews = critic_reviews[['CriticReviewID', 'CriticID', 'GameID', 'PlatformID', 'review_text', 'critic_score']]

critic_reviews = critic_reviews[critic_reviews['critic_score'] != 'tbd']

# Set explicit data types
critic_reviews['CriticReviewID'] = critic_reviews['CriticReviewID'].astype(int)  
critic_reviews['CriticID'] = critic_reviews['CriticID'].astype(int)  
critic_reviews['GameID'] = critic_reviews['GameID'].astype(int)  
critic_reviews['PlatformID'] = critic_reviews['PlatformID'].astype(int)  
critic_reviews['review_text'] = critic_reviews['review_text'].astype(str) 
critic_reviews['critic_score'] = critic_reviews['critic_score'].astype(int)  

# Fill missing game titles with a placeholder or drop them
critic_reviews['review_text'] = critic_reviews['review_text'].fillna('Unknown')
critic_reviews['critic_score'] = critic_reviews['critic_score'].fillna('Unknown')

critic_reviews.set_index('CriticReviewID', inplace=True)

# Save to a new CSV
critic_reviews.to_csv(r'Transformed Files\CriticReviewsTable.csv')
print(critic_reviews.head())



# User Review Table
df = pd.read_csv(r'Raw Files\one_row_one_review_users.csv')

# Define  UsersID, GamesID, and PlatformID DataFrames
UsersID = pd.read_csv(r'Transformed Files\UserID.csv') 
GamesID = pd.read_csv(r'Transformed Files\GameID.csv') 
PlatformsID = pd.read_csv(r'Transformed Files\PlatformID.csv') 

# Merge UsersID, GameID, and PlatformsID with the raw data
user_reviews = df.merge(UsersID, on='username').merge(GamesID, on='game_title').merge(PlatformsID, on='platform')

# Create Identifier
user_reviews['UserReviewID'] = user_reviews.index + 1 

# Keep only the relevant columns
user_reviews = user_reviews[['UserReviewID', 'UserID', 'GameID', 'PlatformID', 'review_text', 'user_score']]

# Set explicit data types
user_reviews['UserReviewID'] = user_reviews['UserReviewID'].astype(int)  
user_reviews['UserID'] = user_reviews['UserID'].astype(int)  
user_reviews['GameID'] = user_reviews['GameID'].astype(int)  
user_reviews['PlatformID'] = user_reviews['PlatformID'].astype(int)  
user_reviews['review_text'] = user_reviews['review_text'].astype(str) 
user_reviews['user_score'] = user_reviews['user_score'].astype(int)  

# Fill missing game titles with a placeholder or drop them
user_reviews['review_text'] = user_reviews['review_text'].fillna('Unknown')
user_reviews['user_score'] = user_reviews['user_score'].fillna('Unknown')

user_reviews.set_index('UserReviewID', inplace=True)

# Save to a new CSV
user_reviews.to_csv(r'Transformed Files\UserReviewsTable.csv')
print(user_reviews.head())



# Ratings Table 
df = pd.read_csv(r'Raw Files\completed_records_plus_sentiment_metrics.csv')

# Define GamesID DataFrames
GamesID = pd.read_csv(r'Transformed Files\GameID.csv')  

# Merge GameID with the raw data
ratings = df.merge(GamesID, on='game_title')

# Keep only the relevant columns
ratings = ratings[['GameID', 'metascore', 'global_user_score_mean', 'user_score_mean']]

# Set explicit data types
ratings['GameID'] = ratings['GameID'].astype(int)  
ratings['metascore'] = ratings['metascore'].astype(int)  
ratings['global_user_score_mean'] = ratings['global_user_score_mean'].astype(float)  
ratings['user_score_mean'] = ratings['user_score_mean'].astype(float)  

ratings['RID'] = range(1, len(ratings) + 1)
ratings.set_index('RID', inplace=True)

# Save to a new CSV
ratings.to_csv(r'Transformed Files\RatingsTable.csv')
print(ratings.head())



# Sentiment Scores
df = pd.read_csv(r'Raw Files\completed_records_plus_sentiment_metrics.csv')

# Define your GamesID DataFrames
GamesID = pd.read_csv(r'Transformed Files\GameID.csv')  

# Merge GameID with the raw data
sentiment_scores = df.merge(GamesID, on='game_title')

# Keep only the relevant columns
sentiment_scores = sentiment_scores[['GameID', 'vader_sentiment_mean', 'textblob_sentiment_mean', 'hf_sentiment_mean']]

# Set explicit data types
sentiment_scores['GameID'] = sentiment_scores['GameID'].astype(int)  
sentiment_scores['vader_sentiment_mean'] = sentiment_scores['vader_sentiment_mean'].astype(float)  
sentiment_scores['textblob_sentiment_mean'] = sentiment_scores['textblob_sentiment_mean'].astype(float)  
sentiment_scores['hf_sentiment_mean'] = sentiment_scores['hf_sentiment_mean'].astype(float)

sentiment_scores['SSID'] = range(1, len(sentiment_scores) + 1)
sentiment_scores.set_index('SSID', inplace=True)

# Save to a new CSV
sentiment_scores.to_csv(r'Transformed Files\SentimentScoresTable.csv')
print(sentiment_scores.head())



# Composite Sentiment Scores Table
df = pd.read_csv(r'Raw Files\completed_records_plus_sentiment_metrics.csv')

# Define your GamesID DataFrames
GameID = pd.read_csv(r'Transformed Files\GameID.csv') 

# Merge GameID with the raw data
composite_sentiment_scores = df.merge(GameID, on='game_title')

# Keep only the relevant columns
composite_sentiment_scores = composite_sentiment_scores[['GameID', 'composite_sentiment_correlation_user_mean',
       'composite_sentiment_correlation_global_mean',
       'composite_sentiment_confidence_mean',
       'composite_sentiment_adaptive_mean']]

composite_sentiment_scores = composite_sentiment_scores.rename(columns={'composite_sentiment_correlation_user_mean': 'comp_sent_corr_user',
                       'composite_sentiment_correlation_global_mean': 'comp_sent_corr_global',
                       'composite_sentiment_confidence_mean': 'comp_sent_confidence', 
                       'composite_sentiment_adaptive_mean': 'comp_sent_adaptive'})

# Set explicit data types
composite_sentiment_scores['GameID'] = composite_sentiment_scores['GameID'].astype(int)  
composite_sentiment_scores['comp_sent_corr_user'] = composite_sentiment_scores['comp_sent_corr_user'].astype(float)  
composite_sentiment_scores['comp_sent_corr_global'] = composite_sentiment_scores['comp_sent_corr_global'].astype(float)  
composite_sentiment_scores['comp_sent_confidence'] = composite_sentiment_scores['comp_sent_confidence'].astype(float)
composite_sentiment_scores['comp_sent_adaptive'] = composite_sentiment_scores['comp_sent_adaptive'].astype(float)

composite_sentiment_scores['CSSID'] = range(1, len(composite_sentiment_scores) + 1)
composite_sentiment_scores.set_index('CSSID', inplace=True)

# Save to a new CSV
composite_sentiment_scores.to_csv(r'Transformed Files\CompositeSentimentScoresTable.csv')
print(composite_sentiment_scores.head())



# Gameplay Table
df = pd.read_csv(r'Raw Files\completed_records_plus_sentiment_metrics.csv')

# Define GamesID DataFrames
GameID = pd.read_csv(r'Transformed Files\GameID.csv') 

# Merge GameID with the raw data
gameplay = df.merge(GameID, on='game_title')

# Calculate difficulty average
gameplay['difficulty_avg'] = gameplay.apply(
    lambda x: (
        x['Difficulty_Easy'] * 1 +
        x['Difficulty_Just_Right'] * 2 +
        x['Difficulty_Simple'] * 1 +
        x['Difficulty_Tough'] * 3 +
        x['Difficulty_Unforgiving'] * 3
    ) / 100, axis=1
)

# Calculate completion average
gameplay['completion_avg'] = gameplay.apply(
    lambda x: (
        x['Completion_Tried_It'] * 1 +
        x['Completion_Beat_It'] * 4 +
        x['Completion_Conquered_It'] * 4 +
        x['Completion_Halfway'] * 2 +
        x['Completion_Played_It'] * 1
    ) / 100, axis=1
)

# Calculate playtime average
gameplay['playtime_avg'] = gameplay.apply(
    lambda x: (
        x['Play Time_<_1_Hour'] * 1 +
        x['Play Time_>=_80_Hours'] * 80 +
        x['Play Time_~1_Hour'] * 0.5 +
        x['Play Time_~12_Hours'] * 12 +
        x['Play Time_~2_Hours'] * 2 +
        x['Play Time_~20_Hours'] * 20 +
        x['Play Time_~4_Hours'] * 4 +
        x['Play Time_~40_Hours'] * 40 +
        x['Play Time_~60_Hours'] * 60 +
        x['Play Time_~8_Hours'] * 8
    ) / 100, axis=1
)

# Rename Vars
gameplay.rename(columns={
    'GameID': 'GameID',
    'Difficulty_Easy': 'difficulty_easy',
    'Difficulty_Just_Right': 'difficulty_just_right',
    'Difficulty_Simple': 'difficulty_simple',
    'Difficulty_Tough': 'difficulty_tough',
    'Difficulty_Unforgiving': 'difficulty_unforgiving',
    'Play Time_<_1_Hour': 'play_time_lt_1_hour',
    'Play Time_>=_80_Hours': 'play_time_ge_80_hours',
    'Play Time_~1_Hour': 'play_time_approx_1_hour',
    'Play Time_~12_Hours': 'play_time_approx_12_hours',
    'Play Time_~2_Hours': 'play_time_approx_2_hours',
    'Play Time_~20_Hours': 'play_time_approx_20_hours',
    'Play Time_~4_Hours': 'play_time_approx_4_hours',
    'Play Time_~40_Hours': 'play_time_approx_40_hours',
    'Play Time_~60_Hours': 'play_time_approx_60_hours',
    'Play Time_~8_Hours': 'play_time_approx_8_hours',
    'Completion_Beat_It': 'completion_beat_it',
    'Completion_Conquered_It': 'completion_conquered_it',
    'Completion_Halfway': 'completion_halfway',
    'Completion_Played_It': 'completion_played_it',
    'Completion_Tried_It': 'completion_tried_it',
    'difficulty_avg': 'difficulty_avg',
    'completion_avg': 'completion_avg',
    'playtime_avg': 'playtime_avg'
}, inplace=True)

# Keep only the relevant columns
gameplay = gameplay[['GameID', 'difficulty_easy', 'difficulty_just_right',
       'difficulty_simple', 'difficulty_tough', 'difficulty_unforgiving',
       'play_time_lt_1_hour', 'play_time_ge_80_hours', 'play_time_approx_1_hour',
       'play_time_approx_12_hours', 'play_time_approx_2_hours', 'play_time_approx_20_hours',
       'play_time_approx_4_hours', 'play_time_approx_40_hours', 'play_time_approx_60_hours',
       'play_time_approx_8_hours', 'completion_beat_it', 'completion_conquered_it',
       'completion_halfway', 'completion_played_it', 'completion_tried_it', 'difficulty_avg', 
       'completion_avg', 'playtime_avg']]

gameplay['GameID'] = gameplay['GameID'].astype(int)  
gameplay['difficulty_easy'] = gameplay['difficulty_easy'].astype(float) 
gameplay['difficulty_just_right'] = gameplay['difficulty_just_right'].astype(float)  
gameplay['difficulty_simple'] = gameplay['difficulty_simple'].astype(float)  
gameplay['difficulty_tough'] = gameplay['difficulty_tough'].astype(float)
gameplay['difficulty_unforgiving'] = gameplay['difficulty_unforgiving'].astype(float)
gameplay['play_time_lt_1_hour'] = gameplay['play_time_lt_1_hour'].astype(float)  
gameplay['play_time_ge_80_hours'] = gameplay['play_time_ge_80_hours'].astype(float)  
gameplay['play_time_approx_1_hour'] = gameplay['play_time_approx_1_hour'].astype(float)  
gameplay['play_time_approx_12_hours'] = gameplay['play_time_approx_12_hours'].astype(float)
gameplay['play_time_approx_2_hours'] = gameplay['play_time_approx_2_hours'].astype(float)
gameplay['play_time_approx_20_hours'] = gameplay['play_time_approx_20_hours'].astype(float)
gameplay['play_time_approx_4_hours'] = gameplay['play_time_approx_4_hours'].astype(float)
gameplay['play_time_approx_40_hours'] = gameplay['play_time_approx_40_hours'].astype(float)
gameplay['play_time_approx_60_hours'] = gameplay['play_time_approx_60_hours'].astype(float)  
gameplay['play_time_approx_8_hours'] = gameplay['play_time_approx_8_hours'].astype(float)  
gameplay['completion_beat_it'] = gameplay['completion_beat_it'].astype(float)  
gameplay['completion_conquered_it'] = gameplay['completion_conquered_it'].astype(float)
gameplay['completion_halfway'] = gameplay['completion_halfway'].astype(float)
gameplay['completion_played_it'] = gameplay['completion_played_it'].astype(float)  
gameplay['completion_tried_it'] = gameplay['completion_tried_it'].astype(float)  
gameplay['difficulty_avg'] = gameplay['difficulty_avg'].astype(float)  
gameplay['completion_avg'] = gameplay['completion_avg'].astype(float)
gameplay['playtime_avg'] = gameplay['playtime_avg'].astype(float)

gameplay['GPID'] = range(1, len(gameplay) + 1)
gameplay.set_index('GPID', inplace=True)

# Save to a new CSV
gameplay.to_csv(r'Transformed Files\GameplayTable.csv')
print(gameplay.head())



# GAME VARIABLES
# GameVars Table
df = pd.read_csv(r'Raw Files\completed_records_plus_sentiment_metrics.csv')

# Define GamesID DataFrames
GamesID = pd.read_csv(r'Transformed Files\GameID.csv') 

# Merge GameID with the raw data
game_vars = df.merge(GamesID, on='game_title')

# Keep only the relevant columns
game_vars = game_vars[['GameID', 'Year', 'Genre', 'Publisher','developer',  
                     'Total_Sales', 'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 
                     'user_review_count', 'critic_review_count']]

game_vars['GameID'] = game_vars['GameID'].astype(int)  
game_vars['Year'] = game_vars['Year'].astype(int) 
game_vars['Genre'] = game_vars['Genre'].astype(str)  
game_vars['Publisher'] = game_vars['Publisher'].astype(str) 
game_vars['developer'] = game_vars['developer'].astype(str)  
game_vars['Total_Sales'] = game_vars['Total_Sales'].astype(float) 
game_vars['NA_Sales'] = game_vars['NA_Sales'].astype(float)  
game_vars['EU_Sales'] = game_vars['EU_Sales'].astype(float) 
game_vars['JP_Sales'] = game_vars['JP_Sales'].astype(float)  
game_vars['Other_Sales'] = game_vars['Other_Sales'].astype(float) 
game_vars['user_review_count'] = game_vars['user_review_count'].astype(int)  
game_vars['critic_review_count'] = game_vars['critic_review_count'].astype(int) 

# Create a unique ID for the new table
game_vars['GVID'] = range(1, len(game_vars) + 1)
game_vars.set_index('GVID', inplace=True)

# Save to a new CSV
game_vars.to_csv(r'Transformed Files\GameVarsTable.csv')
print(game_vars.head())



# GamePlatformSentiment Table
# Load data
df = pd.read_csv(r'Raw Files\title_platform_user_and_critics_sentiment.csv')

# Function to clean percentage columns
def clean_percentage_column(df, column):
    # Remove '%' and convert to float
    df[column] = df[column].replace('%', '', regex=True).astype(float)
    # Convert percentages to decimals
    df[column] = df[column] / 100
    return df

# Function to clean count columns
def clean_count_column(df, column):
    # Replace 'k' with an empty string and multiply the number by 1000
    df[column] = df[column].astype(str)  # Ensure values are strings for regex
    df[column] = df[column].str.replace(r'(\d+(\.\d+)?)[kK]', lambda x: str(float(x.group(1)) * 1000), regex=True)
    # Convert the cleaned column to float
    df[column] = df[column].astype(float)
    return df

# List of percentage columns
percentage_columns = [
    'user_positive_percent', 'user_mixed_percent', 'user_negative_percent',
    'critic_positive_percent', 'critic_mixed_percent', 'critic_negative_percent'
]

# List of count columns
count_columns = [
    'user_positive_count', 'user_mixed_count', 'user_negative_count',
    'critic_positive_count', 'critic_mixed_count', 'critic_negative_count'
]

# Apply the percentage transformation to percentage columns
for column in percentage_columns:
    df = clean_percentage_column(df, column)

# Apply the count transformation to count columns
for column in count_columns:
    df = clean_count_column(df, column)


# Read in GamePlatformID
GPID = pd.read_csv(r'Transformed Files\GamePlatformID.csv')

# Merge with GamePlatformID using both 'game_title' and 'platform'
sentiment_data = df.merge(GPID, on=['game_title', 'platform'])

# Create a unique ID for the new table
sentiment_data['GPSID'] = range(1, len(sentiment_data) + 1)

# Select and reorder columns
game_platform_sentiment = sentiment_data[
    ['GPSID', 'GamePlatformID', 'game_title', 'platform',
     'user_positive_count', 'user_positive_percent', 'user_mixed_count',
     'user_mixed_percent', 'user_negative_count', 'user_negative_percent',
     'critic_positive_count', 'critic_positive_percent', 'critic_mixed_count',
     'critic_mixed_percent', 'critic_negative_count', 'critic_negative_percent']
]
# Drop rows with any missing values
game_platform_sentiment = game_platform_sentiment.dropna()

game_platform_sentiment.loc[:, 'GPSID'] = game_platform_sentiment['GPSID'].astype(int)
game_platform_sentiment.loc[:, 'GamePlatformID'] = game_platform_sentiment['GamePlatformID'].astype(int)
game_platform_sentiment.loc[:, 'game_title'] = game_platform_sentiment['game_title'].astype(str)
game_platform_sentiment.loc[:, 'platform'] = game_platform_sentiment['platform'].astype(str)
game_platform_sentiment.loc[:, 'user_positive_count'] = game_platform_sentiment['user_positive_count'].astype(int)
game_platform_sentiment.loc[:, 'user_positive_percent'] = game_platform_sentiment['user_positive_percent'].astype(float)
game_platform_sentiment.loc[:, 'user_mixed_count'] = game_platform_sentiment['user_mixed_count'].astype(int)
game_platform_sentiment.loc[:, 'user_mixed_percent'] = game_platform_sentiment['user_mixed_percent'].astype(float)
game_platform_sentiment.loc[:, 'user_negative_count'] = game_platform_sentiment['user_negative_count'].astype(int)
game_platform_sentiment.loc[:, 'user_negative_percent'] = game_platform_sentiment['user_negative_percent'].astype(float)
game_platform_sentiment.loc[:, 'critic_positive_count'] = game_platform_sentiment['critic_positive_count'].astype(int)
game_platform_sentiment.loc[:, 'critic_positive_percent'] = game_platform_sentiment['critic_positive_percent'].astype(float)
game_platform_sentiment.loc[:, 'critic_mixed_count'] = game_platform_sentiment['critic_mixed_count'].astype(int)
game_platform_sentiment.loc[:, 'critic_mixed_percent'] = game_platform_sentiment['critic_mixed_percent'].astype(float)
game_platform_sentiment.loc[:, 'critic_negative_count'] = game_platform_sentiment['critic_negative_count'].astype(int)
game_platform_sentiment.loc[:, 'critic_negative_percent'] = game_platform_sentiment['critic_negative_percent'].astype(float)

# Set 'GPSID' as the index 
game_platform_sentiment.set_index('GPSID', inplace=True)

# Save to CSV
game_platform_sentiment.to_csv(r'Transformed Files\GamePlatformSentimentTable.csv')

# Print the head of the new table for verification
print(game_platform_sentiment.head())

