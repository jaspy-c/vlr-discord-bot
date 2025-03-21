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
        while True:
            async with self.lock:
                now = time.time()
                # Remove timestamps older than cooldown period
                self.request_times = [t for t in self.request_times if now - t < self.cooldown_period]
                
                if len(self.request_times) < self.max_requests:
                    # We are under the limit, so record the current time and break out
                    self.request_times.append(now)
                    break
                else:
                    # Calculate time to wait based on the oldest request timestamp
                    wait_time = self.request_times[0] + self.cooldown_period - now + 0.1
                    print(f"⏱️ Rate limit hit, waiting for {wait_time:.2f} seconds")
            # Release the lock before sleeping
            await asyncio.sleep(wait_time)


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

# Connect to PostgreSQL database and get a connection
def get_db_connection():
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
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {str(e)}")
        traceback.print_exc()
        return None

# Function to create tables if they don't exist
def initialize_database():
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP,
                team1 VARCHAR(255),
                team2 VARCHAR(255),
                score1 VARCHAR(10),
                score2 VARCHAR(10),
                status VARCHAR(50),
                phase VARCHAR(255),
                tournament VARCHAR(255),
                match_link VARCHAR(255) UNIQUE,
                notified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Database initialized successfully")
        return True
    except Exception as e:
        print(f"❌ Error initializing database: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.close()
        return False

# Function to get existing notified matches from database
def get_notified_matches():
    conn = get_db_connection()
    if not conn:
        return set()
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT match_link FROM matches WHERE notified = TRUE")
        notified_matches = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        print(f"✅ Found {len(notified_matches)} previously notified matches in database")
        return notified_matches
    except Exception as e:
        print(f"❌ Error getting notified matches: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.close()
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
    
    # Get matches we've already notified about
    notified_matches = get_notified_matches()

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
            if not ("champions" in match_link or "masters" in match_link):
                continue

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

            match_status = element.find("div", class_="ml-status").text.strip().lower()

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
            if match_status.lower() in ["completed", "finished", "final"] and match_link not in notified_matches:
                # Make sure the match date is recent (within the last 24 hours)
                try:
                    match_date = datetime.strptime(formatted_datetime, "%Y-%m-%d %H:%M:%S")
                    now = datetime.now()
                    time_diff = now - match_date
                    
                    # Only notify about matches from the last 24 hours
                    if time_diff.total_seconds() < 86400:  # 24 hours in seconds
                        new_completed_matches.append(match_data)
                    else:
                        print(f"Match is too old for notification ({time_diff.total_seconds()/3600:.1f} hours): {team1} vs {team2}")
                except ValueError:
                    # If we can't parse the date, skip notification
                    print(f"Could not parse date for match: {team1} vs {team2}")

    print(f"Found a total of {len(matches)} matches ({len(new_completed_matches)} new completed)")

    # Update Google Sheets with all match data
    update_google_sheets(matches)
    
    # Update PostgreSQL database with all matches
    insert_data_to_db(matches, new_completed_matches)

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
def insert_data_to_db(matches, new_completed_matches=None):
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Process each match (without truncating existing data)
        for match in matches:
            # Extract match link for identification
            match_link = match[8]
            
            # Get current match status
            match_status = match[5].lower()  # Status is at index 5
            
            # Check if this match is completed and should be notified
            should_notify = match_status in ["completed", "finished", "final"]
            
            # Insert or update match record
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
                    tournament = EXCLUDED.tournament,
                    -- Only update notified if it's not already TRUE
                    notified = CASE 
                                WHEN matches.notified = TRUE THEN TRUE 
                                ELSE FALSE 
                              END
            """, match)  # Just use match as is, without adding the FALSE parameter
            
            # If debugging, print match status
            print(f"Match {match[1]} vs {match[3]} - Status: {match_status}, Should notify: {should_notify}")
            
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Updated PostgreSQL database with", len(matches), "matches!")
    except Exception as e:
        print(f"❌ Error inserting data into PostgreSQL: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.close()
# Get matches to notify about (WITHOUT marking them as notified yet)
def get_matches_for_notification():
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        # Get matches that need notification
        cur.execute("""
            SELECT datetime, team1, score1, team2, score2, status, phase, tournament, match_link
            FROM matches 
            WHERE status IN ('completed', 'finished', 'final')
            AND notified = FALSE
        """)
        matches_to_notify = cur.fetchall()
        cur.close()
        conn.close()
        
        if matches_to_notify:
            print(f"✅ Found {len(matches_to_notify)} matches that need notification")
        else:
            print("No matches need notification at this time")
            
        return matches_to_notify
    except Exception as e:
        print(f"❌ Error getting matches for notification: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.close()
        return []

# Mark matches as notified (only call this after notifications are sent successfully)
def mark_matches_as_notified(match_links):
    if not match_links:
        return 0
        
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        cur = conn.cursor()
        placeholders = ','.join(['%s'] * len(match_links))
        cur.execute(f"UPDATE matches SET notified = TRUE WHERE match_link IN ({placeholders})", match_links)
        rows_affected = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Marked {rows_affected} matches as notified")
        return rows_affected
    except Exception as e:
        print(f"❌ Error marking matches as notified: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.close()
        return 0

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

    # Initialize database
    initialize_database()
    
    # Start the match checking task
    check_for_new_matches.start()

# Task to periodically check for matches
@tasks.loop(minutes=10)  # Check every 10 minutes
async def check_for_new_matches():
    print(f"🔄 Checking for new matches... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        # First, scrape VLR and update both databases
        scrape_vlr()
        
        # Then, get matches that need notification
        matches_to_notify = get_matches_for_notification()

        if matches_to_notify:
            print(f"Found {len(matches_to_notify)} matches to notify about!")

            if target_user:
                try:
                    successful_notifications = []  # Track which notifications were successful
                    
                    for match_data in matches_to_notify:
                        match_message = (
                            f"\n"
                            f"🏆 {match_data[7]} - {match_data[6]}\n"
                            f"⚔️ {match_data[1]} vs {match_data[3]}\n"
                            f"Score: {match_data[2]} - {match_data[4]}\n"
                            f"🕒 {match_data[0]}\n"
                            f"🔗 [Match Link]({match_data[8]})"
                        )
                        try:
                            # Use the rate-limited message sending function
                            await send_rate_limited_message(target_user, match_message)
                            # If successful, add to our successful list
                            successful_notifications.append(match_data[8])  # Add match link
                        except Exception as e:
                            print(f"❌ Failed to send notification for {match_data[1]} vs {match_data[3]}: {str(e)}")
                    
                    # Only mark matches as notified if we successfully sent the notification
                    if successful_notifications:
                        mark_matches_as_notified(successful_notifications)
                        print(f"✅ Successfully sent and marked {len(successful_notifications)} notifications!")
                    
                except discord.errors.Forbidden:
                    print("❌ Cannot send DM to user - they have DMs disabled or blocked the bot")
                except Exception as e:
                    print(f"❌ Error sending notifications: {str(e)}")
                    traceback.print_exc()
            else:
                print(f"❌ Target user not found. Still waiting to find user with ID {YOUR_DISCORD_USER_ID}")
        else:
            print("✅ No new matches to notify about.")
    except Exception as e:
        print(f"❌ Error in check_for_new_matches: {str(e)}")
        traceback.print_exc()

# Manual command to force notification check
@bot.command(name="checkmatches")
async def force_check_matches(ctx):
    """Manually trigger a check for new matches"""
    if ctx.author.id == int(YOUR_DISCORD_USER_ID):
        await ctx.send("🔄 Manually checking for new matches...")
        try:
            await check_for_new_matches()
            await ctx.send("✅ Manual check completed!")
        except Exception as e:
            await ctx.send(f"❌ Error during manual check: {str(e)}")
    else:
        await ctx.send("Only the bot owner can use this command.")

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

# Reset notification status (for debugging)
@bot.command(name="resetnotifications")
async def reset_notifications(ctx):
    """Reset notification status for all matches"""
    if ctx.author.id == int(YOUR_DISCORD_USER_ID):
        conn = get_db_connection()
        if not conn:
            await ctx.send("❌ Could not connect to database")
            return
            
        try:
            cur = conn.cursor()
            cur.execute("UPDATE matches SET notified = FALSE")
            conn.commit()
            rows_affected = cur.rowcount
            cur.close()
            conn.close()
            await ctx.send(f"✅ Reset notification status for {rows_affected} matches!")
        except Exception as e:
            await ctx.send(f"❌ Error: {str(e)}")
            if conn:
                conn.close()
    else:
        await ctx.send("Only the bot owner can use this command.")
        
# List unnotified matches (for debugging)
@bot.command(name="pendingmatches")
async def list_pending_matches(ctx):
    """List matches that are pending notification"""
    if ctx.author.id == int(YOUR_DISCORD_USER_ID):
        conn = get_db_connection()
        if not conn:
            await ctx.send("❌ Could not connect to database")
            return
            
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT team1, team2, score1, score2, status, match_link 
                FROM matches 
                WHERE status IN ('completed', 'finished', 'final')
                AND notified = FALSE
                AND datetime > NOW() - INTERVAL '24 hours'
            """)
            pending_matches = cur.fetchall()
            cur.close()
            conn.close()
            
            if pending_matches:
                message = "📋 **Pending Matches:**\n\n"
                for i, match in enumerate(pending_matches, 1):
                    message += f"{i}. {match[0]} vs {match[1]} ({match[2]}-{match[3]}) - {match[4]}\n"
                    
                    # Discord has a 2000 character limit per message
                    if len(message) > 1900:
                        await ctx.send(message)
                        message = ""
                
                if message:
                    await ctx.send(message)
            else:
                await ctx.send("No pending matches found!")
        except Exception as e:
            await ctx.send(f"❌ Error: {str(e)}")
            if conn:
                conn.close()
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
