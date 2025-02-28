import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import discord
from discord.ext import commands, tasks
import os
import json
from flask import Flask
from threading import Thread
import psycopg2

# Discord bot configuration
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
YOUR_DISCORD_USER_ID = os.getenv("YOUR_DISCORD_USER_ID")  # Your Discord user ID

if not DISCORD_BOT_TOKEN:
    print("ERROR: Discord bot token is missing!")
    exit(1)

# Load Google credentials from environment
google_creds = json.loads(os.getenv("GOOGLE_CREDS_JSON"))

# Initialize Discord bot
intents = discord.Intents.default()
intents.members = True  # Enable members intent
bot = commands.Bot(command_prefix="!", intents=intents)

# Store user object once found
target_user = None

# Connect to Google Sheets
def get_google_sheets_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds, scope)
    return gspread.authorize(creds)

# Function to get existing matches from Google Sheets
def get_existing_matches_from_sheet():
    try:
        client = get_google_sheets_client()
        sheet = client.open("vod_fetcher").sheet1
        
        # Get all rows excluding the header
        all_data = sheet.get_all_values()
        if len(all_data) <= 1:  # Only header or empty
            return set()
            
        # Extract URLs from the last column (Match URL)
        match_urls = set()
        for row in all_data[1:]:  # Skip header row
            if len(row) >= 9:  # Make sure we have enough columns
                match_urls.add(row[8])  # URL is in the 9th column (index 8)
        
        print(f"✅ Found {len(match_urls)} existing matches in Google Sheet")
        return match_urls
    except Exception as e:
        print(f"❌ Error getting existing matches: {e}")
        return set()

# Function to scrape VLR.gg and update data
def scrape_vlr():
    VLR_URL = "https://www.vlr.gg/matches/results"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(VLR_URL, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    matches = []
    new_completed_matches = []  # Store newly completed matches for notifications

    current_date = None  # To track the current match date

    # Get existing match URLs from the Google Sheet
    existing_match_urls = get_existing_matches_from_sheet()

    # Iterate through the match schedule
    for element in soup.find_all(["div", "a"]):
        if set(element.get("class", [])) == {"wf-label", "mod-large"}:
            # Update current_date whenever we find a new date header
            current_date = element.text.strip().split("\n")[0]  # Extract only the date text

        if "wf-module-item" in element.get("class", []) and "mod-bg-after-striped_purple" in element.get("class", []):
            match_link = "https://www.vlr.gg" + element["href"]

            # Check if match is already in our sheet
            is_new_match = match_link not in existing_match_urls

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
                formatted_datetime, team1, score1, team2, score5, match_status,
                phase, tournament, match_link
            ]
            matches.append(match_data)

            # Check if this is a newly completed match that we should notify about
            if is_new_match and match_status.lower() in ["completed", "finished", "final"]:
                new_completed_matches.append(match_data)

    # Update Google Sheets with all match data
    update_google_sheets(matches)
    
    # Update PostgreSQL database with all matches
    insert_data_to_db(matches)

    return new_completed_matches

# Update Google Sheets with match data
def update_google_sheets(matches):
    try:
        client = get_google_sheets_client()

        # Update main data sheet
        sheet = client.open("vod_fetcher").sheet1
        
        # Clear all data except header
        sheet.clear()
        sheet.append_row([
            "Datetime", "Team 1", "Score 1", "Team 2", "Score 2", "Status",
            "Phase", "Tournament", "Match URL"
        ])
        
        # Add all current matches
        if matches:
            sheet.append_rows(matches)
            
        print("✅ Updated Google Sheets with", len(matches), "matches!")
    except Exception as e:
        print(f"❌ Error updating Google Sheets: {e}")

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
        
        # Create table if it doesn't exist
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP,
                team1 VARCHAR(255),
                score1 VARCHAR(10),
                team2 VARCHAR(255),
                score2 VARCHAR(10),
                status VARCHAR(50),
                phase VARCHAR(255),
                tournament VARCHAR(255),
                match_link VARCHAR(255) UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        
        # Clear existing data and insert new data
        cur.execute("TRUNCATE TABLE matches")
        
        for match in matches:
            cur.execute("""
                INSERT INTO matches 
                (datetime, team1, score1, team2, score2, status, phase, tournament, match_link)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (match_link) DO UPDATE 
                SET datetime = EXCLUDED.datetime,
                    team1 = EXCLUDED.team1,
                    score1 = EXCLUDED.score1,
                    team2 = EXCLUDED.team2,
                    score2 = EXCLUDED.score2,
                    status = EXCLUDED.status,
                    phase = EXCLUDED.phase,
                    tournament = EXCLUDED.tournament
            """, match)
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Updated PostgreSQL database with", len(matches), "matches!")
    except Exception as e:
        print(f"❌ Error inserting data into PostgreSQL: {e}")

# Keep the Flask app alive (required for hosting services like Replit)
def keep_alive():
    app = Flask('')
    
    @app.route('/')
    def home():
        return "I'm alive!"
    
    def run():
        app.run(host='0.0.0.0', port=8080)
    
    server_thread = Thread(target=run)
    server_thread.daemon = True
    server_thread.start()
    print("Flask server started")

# Discord bot events
@bot.event
async def on_ready():
    global target_user
    print(f"Bot is logged in as {bot.user}")

    try:
        target_user = await bot.fetch_user(int(YOUR_DISCORD_USER_ID))
        print(f"✅ Successfully found user: {target_user.name}")
    except discord.errors.NotFound:
        print(f"❌ Could not find user with ID {YOUR_DISCORD_USER_ID}. Check if the ID is correct.")

    # Start the match checking task
    check_for_new_matches.start()

# Task to periodically check for matches
@tasks.loop(minutes=10)  # Check every 10 minutes
async def check_for_new_matches():
    print(f"🔄 Checking for new matches... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    new_completed_matches = scrape_vlr()

    if new_completed_matches:
        print(f"Found {len(new_completed_matches)} new completed matches!")

        if target_user:
            try:
                for match_data in new_completed_matches:
                    match_message = (
                        f"🔥 **New Match Completed!** 🔥\n"
                        f"🏆 {match_data[7]} - {match_data[6]}\n"
                        f"⚔️ {match_data[1]} vs {match_data[3]}\n"
                        f"Score: {match_data[2]} - {match_data[4]}\n"
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
