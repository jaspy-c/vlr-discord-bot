import os
import json
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import discord
from discord.ext import commands, tasks
from flask import Flask
from threading import Thread
from sqlalchemy import create_engine, Column, String, Table, MetaData
from sqlalchemy.orm import sessionmaker

# Load environment variables
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")  # PostgreSQL URL from Render

# Set up database connection
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define table
processed_matches_table = Table(
    "processed_matches", metadata,
    Column("match_link", String, primary_key=True)
)

metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Discord bot setup
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# Function to get processed matches from DB
def get_processed_matches():
    with engine.connect() as conn:
        result = conn.execute(processed_matches_table.select())
        return {row.match_link for row in result}

# Function to save processed matches to DB
def save_processed_match(match_link):
    with engine.connect() as conn:
        conn.execute(processed_matches_table.insert().values(match_link=match_link))
        conn.commit()

# Function to scrape matches
def scrape_vlr_matches():
    VLR_URL = "https://www.vlr.gg/matches/results"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(VLR_URL, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    new_completed_matches = []
    processed_matches = get_processed_matches()

    for element in soup.find_all("a", class_="wf-module-item mod-bg-after-striped_purple"):
        match_link = "https://www.vlr.gg" + element["href"]

        if match_link in processed_matches:
            continue  # Skip if already processed

        match_status = element.find("div", class_="ml-status").text.strip()
        if match_status.lower() in ["completed", "finished", "final"]:
            new_completed_matches.append(match_link)
            save_processed_match(match_link)

    return new_completed_matches

# Bot event
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    check_for_matches.start()

# Task to check matches
@tasks.loop(minutes=60)
async def check_for_matches():
    new_matches = scrape_vlr_matches()
    if new_matches:
        print(f"Found {len(new_matches)} new matches!")

# Flask app to keep alive
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"

def run_flask():
    app.run(host="0.0.0.0", port=8080)

if __name__ == "__main__":
    Thread(target=run_flask).start()
    bot.run(DISCORD_BOT_TOKEN)
