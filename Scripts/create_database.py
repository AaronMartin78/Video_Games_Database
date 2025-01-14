import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import declarative_base, Session
import os

# Change to the directory containing your CSV files
os.chdir(r'D:\GitHub_Files\Video_Games_Database_Files')

Base = declarative_base()

# Define the tables with primary and foreign keys

class GameID(Base):
    __tablename__ = 'GameID'
    
    GameID = Column(Integer, primary_key=True)
    game_title = Column(String, nullable=False)

class PlatformID(Base):
    __tablename__ = 'PlatformID'
    
    PlatformID = Column(Integer, primary_key=True)
    platform = Column(String, nullable=False)

class GamePlatformID(Base):
    __tablename__ = 'GamePlatformID'
    
    GamePlatformID = Column(Integer, primary_key=True)
    platform = Column(String, nullable=False)
    game_title = Column(String, nullable=False)
    GameID = Column(Integer, ForeignKey('GameID.GameID'), nullable=False)
    PlatformID = Column(Integer, ForeignKey('PlatformID.PlatformID'), nullable=False)

class CriticID(Base):
    __tablename__ = 'CriticID'
    
    CriticID = Column(Integer, primary_key=True)
    critic_name = Column(String, nullable=False)

class UserID(Base):
    __tablename__ = 'UserID'
    
    UserID = Column(Integer, primary_key=True)
    user_name = Column(String, nullable=False)

class CriticReviewsTable(Base):
    __tablename__ = 'CriticReviewsTable'
    
    CriticReviewID = Column(Integer, primary_key=True)
    CriticID = Column(Integer, ForeignKey('CriticID.CriticID'), nullable=False)
    GameID = Column(Integer, ForeignKey('GameID.GameID'), nullable=False)
    PlatformID = Column(Integer, ForeignKey('PlatformID.PlatformID'), nullable=False)
    review_text = Column(String)
    critic_score = Column(Integer)

class UserReviewsTable(Base):
    __tablename__ = 'UserReviewsTable'
    
    UserReviewID = Column(Integer, primary_key=True)
    UserID = Column(Integer, ForeignKey('UserID.UserID'), nullable=False)
    GameID = Column(Integer, ForeignKey('GameID.GameID'), nullable=False)
    PlatformID = Column(Integer, ForeignKey('PlatformID.PlatformID'), nullable=False)
    review_text = Column(String)
    user_score = Column(Integer)

class RatingsTable(Base):
    __tablename__ = 'RatingsTable'
    
    GameID = Column(Integer, primary_key=True)
    metascore = Column(Integer, nullable=False)
    global_user_score_mean = Column(Float, nullable=False)
    user_score_mean = Column(Float, nullable=False)

class SentimentScoresTable(Base):
    __tablename__ = 'SentimentScoresTable'
    
    GameID = Column(Integer, primary_key=True)
    vader_sentiment_mean = Column(Float, nullable=False)
    textblob_sentiment_mean = Column(Float, nullable=False)
    hf_sentiment_mean = Column(Float, nullable=False)

class CompositeSentimentScoresTable(Base):
    __tablename__ = 'CompositeSentimentScoresTable'
    
    GameID = Column(Integer, primary_key=True)
    comp_sent_corr_user = Column(Float, nullable=False)
    comp_sent_corr_global = Column(Float, nullable=False)
    comp_sent_confidence = Column(Float, nullable=False)
    comp_sent_adaptive  = Column(Float, nullable=False)

class GameplayTable(Base):
    __tablename__ = 'GameplayTable'
    
    GameID = Column(Integer, primary_key=True)
    Difficulty_Easy = Column(Float, nullable=False)
    Difficulty_Just_Right = Column(Float, nullable=False)
    Difficulty_Simple = Column(Float, nullable=False)
    Difficulty_Tough = Column(Float, nullable=False)
    Difficulty_Unforgiving = Column(Float, nullable=False)

# Add other columns as needed...

class GameVarsTable(Base):
    __tablename__ = 'GameVarsTable'
    
    GameID = Column(Integer, primary_key=True)
    Year = Column(Integer, nullable=False)
    Genre = Column(String, nullable=False)

# Add other columns as needed...

class GamePlatformSentimentTable(Base):
    __tablename__ = 'GamePlatformSentimentTable'
    
    GamePlatformSentimentID = Column(Integer, primary_key=True)
    GamePlatformID = Column(Integer, ForeignKey('GamePlatformID.GamePlatformID'), nullable=False)

# Create the database and tables
def main():
    db_url = 'sqlite:///video_games.db'
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    
    csv_files_path_list = [
    r'Transformed Files/GameID.csv',
    r'Transformed Files/PlatformID.csv',
    r'Transformed Files/CriticID.csv',
    r'Transformed Files/CriticReviewsTable.csv',
    r'Transformed Files/GamePlatformID.csv',
    r'Transformed Files/UserID.csv',
    r'Transformed Files/UserReviewsTable.csv',
    r'Transformed Files/CompositeSentimentScoresTable.csv',
    r'Transformed Files/GamePlatformSentimentTable.csv',
    r'Transformed Files/GameplayTable.csv',
    r'Transformed Files/GameVarsTable.csv',
    r'Transformed Files/RatingsTable.csv',
    r'Transformed Files/SentimentScoresTable.csv'
    ]   
    
    with Session(engine) as session:
        for csv_file in csv_files_path_list:
            df=pd.read_csv(csv_file) 
            table_name = os.path.splitext(os.path.basename(csv_file))[0]
            df.to_sql(table_name , engine , if_exists='replace', index=False)  # Load Data into Tables        
            
if __name__ == '__main__':
    main()  
    
    
