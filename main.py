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
import traceback
import time
import asyncio  # Added for async rate limiting

# Discord bot configuration
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
YOUR_DISCORD_USER_ID = os.getenv("YOUR_DISCORD_USER_ID")  # Your Discord user ID

if not DISCORD_BOT_TOKEN:
    print("ERROR: Discord bot token is missing!")
    exit(1)

# Load Google credentials from environment
try:
    google_creds = json.loads(os.getenv("GOOGLE_CREDS_JSON", "{}"))
    if not google_creds:
        print("WARNING: Google credentials JSON is missing or empty")
except json.JSONDecodeError:
    print("ERROR: Google credentials JSON is invalid")
    google_creds = {}

# Initialize Discord bot
intents = discord.Intents.default()
intents.members = True  # Enable members intent
bot = commands.Bot(command_prefix="!", intents=intents)

# Store user object once found
target_user = None

# Rate limiting configuration
class RateLimiter:
    def __init__(self, max_requests=5, cooldown_period=6):
        self.max_requests = max_requests  # Maximum messages in cooldown period
        self.cooldown_period = cooldown_period  # Cooldown period in seconds
        self.request_times = []  # Track timestamps of requests
        self.lock = asyncio.Lock()  # Synchronize access to request_times
    
    async def wait_if_needed(self):
        async with self.lock:
            # Get current time
            now = time.time()
            
            # Remove timestamps older than cooldown period
            self.request_times = [t for t in self.request_times if now - t < self.cooldown_period]
            
            # If we've hit the rate limit, wait
            if len(self.request_times) >= self.max_requests:
                # Calculate time to wait: oldest timestamp + cooldown - current time + small buffer
                wait_time = self.request_times[0] + self.cooldown_period - now + 0.1
                print(f"⏱️ Rate limit hit, waiting for {wait_time:.2f} seconds")
                
                # Release lock while waiting
                await asyncio.sleep(wait_time)
                
                # Re-acquire lock and check again (recursive call)
                await self.wait_if_needed()
            
            # Add current timestamp and proceed
            self.request_times.append(now)

# Create rate limiter instance
discord_rate_limiter = RateLimiter(max_requests=5, cooldown_period=6)

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
        traceback.print_exc()
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

        # Check if element is a match item with either targeted class
        if "wf-module-item" in element.get("class", []) and (
            "mod-bg-after-striped_purple" in element.get("class", []) or 
            "mod-bg-after-orange" in element.get("class", []) or
            "mod-bg-after-blue" in element.get("class", []) or
            "mod-bg-after-red" in element.get("class", [])
        ):
            match_link = "https://www.vlr.gg" + element["href"]

            if "game-changers" in match_link:
                continue
            
            # Skip if champions or masters is not in the URL
            if "champions" or "masters" not in match_link:
                continue

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

            # Track which class was used for this match (for debugging)
            match_class = "purple" if "mod-bg-after-striped_purple" in element.get("class", []) else "orange"

            match_data = [
                formatted_datetime, team1, score1, team2, score2, match_status,
                phase, tournament, match_link
            ]
            matches.append(match_data)

            # Log match info with class for debugging
            print(f"Found match: {team1} vs {team2} [{match_class}] - Status: {match_status}")

            # Check if this is a newly completed match that we should notify about
            if is_new_match and match_status.lower() in ["completed", "finished", "final"]:
                new_completed_matches.append(match_data)

    print(f"Found a total of {len(matches)} matches ({len(new_completed_matches)} new completed)")

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
        traceback.print_exc()

# Insert data into the PostgreSQL database
def insert_data_to_db(matches):
    try:
        # Try to connect using DATABASE_URL first (common in hosting platforms)
        database_url = os.getenv("DATABASE_URL")
        
        if database_url:
            # Connect directly using the URL
            print("Connecting to database using DATABASE_URL")
            conn = psycopg2.connect(database_url)
        else:
            # Fall back to individual connection parameters
            print("Connecting to database using individual parameters")
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
        print(f"❌ Error inserting data into PostgreSQL: {str(e)}")
        traceback.print_exc()
        print("Make sure DATABASE_URL or the individual DB_* environment variables are set")

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

# Self-pinging mechanism to keep the app awake
def self_ping():
    SELF_PING_URL = os.getenv("SELF_PING_URL", "http://localhost:8080/")
    while True:
        try:
            response = requests.get(SELF_PING_URL)
            print(f"Self ping response: {response.status_code}")
        except Exception as e:
            print(f"Self ping failed: {e}")
        time.sleep(300)  # Ping every 5 minutes

# Rate-limited message sending function
async def send_rate_limited_message(user, content):
    # Wait if we need to respect rate limits
    await discord_rate_limiter.wait_if_needed()
    # Send the message
    await user.send(content)
    print(f"📨 Sent message to {user.name}")

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
    except ValueError:
        print(f"❌ Invalid Discord user ID format: {YOUR_DISCORD_USER_ID}")

    # Start the match checking task
    check_for_new_matches.start()

# Task to periodically check for matches
@tasks.loop(minutes=10)  # Check every 10 minutes
async def check_for_new_matches():
    print(f"🔄 Checking for new matches... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
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
                        # Use the rate-limited message sending function
                        await send_rate_limited_message(target_user, match_message)
                    print(f"✅ Sent {len(new_completed_matches)} new match notifications with rate limiting!")
                except discord.errors.Forbidden:
                    print("❌ Cannot send DM to user - they have DMs disabled or blocked the bot")
                except Exception as e:
                    print(f"❌ Error sending notifications: {str(e)}")
                    traceback.print_exc()
            else:
                print(f"❌ Target user not found. Still waiting to find user with ID {YOUR_DISCORD_USER_ID}")
        else:
            print("✅ No new matches found.")
    except Exception as e:
        print(f"❌ Error in check_for_new_matches: {str(e)}")
        traceback.print_exc()

# Manual command to test rate limiting
@bot.command(name="testlimit")
async def test_rate_limit(ctx, count: int = 10):
    """Test the rate limiting by sending multiple messages"""
    if ctx.author.id == int(YOUR_DISCORD_USER_ID):
        await ctx.send(f"Sending {count} test messages with rate limiting...")
        for i in range(count):
            await send_rate_limited_message(ctx.author, f"Test message {i+1} of {count}")
        await ctx.send("All test messages sent!")
    else:
        await ctx.send("Only the bot owner can use this command.")
        
# Run the bot and start the self-pinging mechanism
if __name__ == "__main__":
    keep_alive()  # Keep the Flask app alive
    # Start the self-ping thread
    ping_thread = Thread(target=self_ping)
    ping_thread.daemon = True
    ping_thread.start()
    bot.run(DISCORD_BOT_TOKEN)
