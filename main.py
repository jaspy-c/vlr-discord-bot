import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import discord
from discord.ext import commands, tasks
import os
import json
from keep_alive import keep_alive
import psycopg2
from flask import Flask
from threading import Thread

# Discord bot configuration
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
YOUR_DISCORD_USER_ID = os.getenv("YOUR_DISCORD_USER_ID")  # Your Discord user ID

if not DISCORD_BOT_TOKEN:
    print("ERROR: Discord bot token is missing!")
    exit(1)

# Load Google credentials from Replit Secrets
google_creds = json.loads(os.getenv("GOOGLE_CREDS_JSON"))

# Initialize Discord bot
intents = discord.Intents.default()
intents.members = True  # Enable members intent
bot = commands.Bot(command_prefix="!", intents=intents)

# Store user object once found
target_user = None

# Initialize Flask app
app = Flask(__name__)

def run_flask():
    app.run(host="0.0.0.0", port=8080)

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

# Function to scrape VLR.gg and return new completed matches
def scrape_vlr():
    VLR_URL = "https://www.vlr.gg/matches/results"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(VLR_URL, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    matches = []
    new_completed_matches = []  # Store newly completed matches for notifications

    current_date = None  # To track the current match date

    # Track processed matches (persistent across script runs)
    processed_matches = load_processed_matches()

    # Iterate through the match schedule
    for element in soup.find_all(["div", "a"]):
        if set(element.get("class", [])) == {"wf-label", "mod-large"}:
            # Update current_date whenever we find a new date header
            current_date = element.text.strip().split("\n")[0]  # Extract only the date text

        if "wf-module-item" in element.get("class", []) and "mod-bg-after-striped_purple" in element.get("class", []):
            match_link = "https://www.vlr.gg" + element["href"]

            # Check if match is already processed
            is_new_match = match_link not in processed_matches

            match_time = element.find("div", class_="match-item-time").text.strip()

            # Combine Date and Time
            full_datetime_str = f"{current_date} {match_time}"
            try:
                full_datetime = datetime.strptime(full_datetime_str, "%a, %B %d, %Y %I:%M %p")
                formatted_datetime = full_datetime.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                formatted_datetime = "Invalid Date"

            teams = element.find_all("div", class_="match-item-vs-team-name")
            team1 = teams[0].text.strip() if len(teams) > 0 else "TBD"
            team2 = teams[1].text.strip() if len(teams) > 1 else "TBD"

            scores = element.find_all("div", class_="match-item-vs-team-score")
            score1 = scores[0].text.strip() if len(scores) > 0 else "-"
            score2 = scores[1].text.strip() if len(scores) > 1 else "-"

            match_status = element.find("div", class_="ml-status").text.strip()

            # Extract Phase and Tournament Name
            phase_tournament = element.find("div", class_="match-item-event")
            phase = phase_tournament.find("div", class_="match-item-event-series").text.strip() if phase_tournament else "N/A"
            tournament = phase_tournament.text.strip().replace(phase, "").strip() if phase_tournament else "N/A"

            match_data = [
                formatted_datetime, team1, score1, team2, score2, match_status,
                phase, tournament, match_link
            ]
            matches.append(match_data)

            # Check if this is a newly completed match
            if is_new_match and match_status.lower() in ["completed", "finished", "final"]:
                new_completed_matches.append(match_data)

            # Add match link to processed matches
            processed_matches.add(match_link)

    # Update Google Sheets
    try:
        # Set up Google Sheets API
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds, scope)
        client = gspread.authorize(creds)

        # Update Google Sheet
        sheet = client.open("vod_fetcher").sheet1
        sheet.clear()
        sheet.append_row([
            "Datetime", "Team 1", "Score 1", "Team 2", "Score 2", "Status",
            "Phase", "Tournament", "Match URL"
        ])
        sheet.append_rows(matches)
        print("✅ Updated Google Sheets!")
    except Exception as e:
        print(f"❌ Error updating Google Sheets: {e}")

    save_processed_matches(processed_matches)

    return new_completed_matches

# Load processed matches from file
def load_processed_matches():
    try:
        with open("processed_matches.txt", "r") as f:
            return set(f.read().splitlines())
    except FileNotFoundError:
        return set()

# Save processed matches to file
def save_processed_matches(processed_matches):
    with open("processed_matches.txt", "w") as f:
        f.write("\n".join(processed_matches))

# Insert data into the PostgreSQL database
def insert_data_to_db(matches):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cur = conn.cursor()
        for match in matches:
            cur.execute("""
                INSERT INTO matches (datetime, team1, score1, team2, score2, status, phase, tournament, match_link)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, match)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error inserting data into PostgreSQL: {e}")

# Fetch data from Google Sheets
def fetch_google_sheets_data():
    try:
        # Set up Google Sheets API
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(google_creds, scope)
        client = gspread.authorize(creds)

        sheet = client.open("vod_fetcher").sheet1
        return sheet.get_all_records()  # Fetch all records from Google Sheets
    except Exception as e:
        print(f"❌ Error fetching data from Google Sheets: {e}")
        return []

# Discord bot events
@bot.event
async def on_ready():
    global target_user
    print(f"Bot is logged in as {bot.user}")

    try:
        target_user = await bot.fetch_user(YOUR_DISCORD_USER_ID)
        print(f"✅ Successfully found user: {target_user.name}#{target_user.discriminator}")
    except discord.errors.NotFound:
        print(f"❌ Could not find user with ID {YOUR_DISCORD_USER_ID}. Check if the ID is correct.")

    # Start the match checking task
    check_for_new_matches.start()

# Task to periodically check for matches
@tasks.loop(minutes=10)  # Check every 10 minutes
async def check_for_new_matches():
    print("🔄 Checking for new matches...")
    new_completed_matches = scrape_vlr()

    if new_completed_matches:
        print(f"Found {len(new_completed_matches)} new completed matches!")

        if target_user:
            try:
                for match_data in new_completed_matches:
                    match_message = (
                        f"🔥 **New Match Found!** 🔥\n"
                        f"🏆 {match_data[7]} - {match_data[6]}\n"
                        f"⚔️ {match_data[1]} vs {match_data[3]}\n"
                        f"🕒 {match_data[0]}\n"
                        f"🔗 [Match Link]({match_data[8]})"
                    )
                    await target_user.send(match_message)
                print(f"✅ Sent {len(new_completed_matches)} new match notifications!")
            except discord.errors.Forbidden:
                print("❌ Cannot send DM to user - they have DMs disabled or blocked the bot")
        else:
            print(f"❌ Target user not found. Still waiting to find user with ID {YOUR_DISCORD_USER_ID}")
    else:
        print("✅ No new matches found.")

# Run the bot
if __name__ == "__main__":
    keep_alive()  # Keep the Flask app alive
    bot.run(DISCORD_BOT_TOKEN)

