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

# Discord bot configuration
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
YOUR_DISCORD_USER_ID = os.getenv(
    "YOUR_DISCORD_USER_ID")  # Your Discord user ID

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


# Function to scrape VLR.gg and return new completed matches
def scrape_vlr_matches():
    VLR_URL = "https://www.vlr.gg/matches/results"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(VLR_URL, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    matches = []
    new_completed_matches = [
    ]  # Store newly completed matches for notifications

    current_date = None  # To track the current match date

    # Track processed matches (persistent across script runs)
    processed_matches = load_processed_matches()

    # Iterate through the match schedule
    for element in soup.find_all(["div", "a"]):
        if set(element.get("class", [])) == {"wf-label", "mod-large"}:
            # Update current_date whenever we find a new date header
            current_date = element.text.strip().split("\n")[
                0]  # Extract only the date text

        # Filter by the specific class "wf-module-item mod-bg-after-striped_purple"
        if "wf-module-item" in element.get(
                "class", []) and "mod-bg-after-striped_purple" in element.get(
                    "class", []):
            match_link = "https://www.vlr.gg" + element["href"]

            # Check if match is already processed
            is_new_match = match_link not in processed_matches

            match_time = element.find("div",
                                      class_="match-item-time").text.strip()

            # Combine Date and Time
            full_datetime_str = f"{current_date} {match_time}"
            try:
                full_datetime = datetime.strptime(full_datetime_str,
                                                  "%a, %B %d, %Y %I:%M %p")
                formatted_datetime = full_datetime.strftime(
                    "%Y-%m-%d %H:%M:%S")
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
            phase = phase_tournament.find(
                "div", class_="match-item-event-series").text.strip(
                ) if phase_tournament else "N/A"
            tournament = phase_tournament.text.strip().replace(
                phase, "").strip() if phase_tournament else "N/A"

            match_data = [
                formatted_datetime, team1, score1, team2, score2, match_status,
                phase, tournament, match_link
            ]
            matches.append(match_data)

            # Check if this is a newly completed match
            if is_new_match and match_status.lower() in [
                    "completed", "finished", "final"
            ]:
                new_completed_matches.append(match_data)

            # Add match link to processed matches
            processed_matches.add(match_link)

    # Update Google Sheets
    try:
        # Set up Google Sheets API
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            google_creds, scope)
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

    # Save the processed matches to the file for future persistence
    save_processed_matches(processed_matches)

    return new_completed_matches


# Discord bot events
@bot.event
async def on_ready():
    global target_user
    print(f"Bot is logged in as {bot.user}")

    # Try to get the user from fetch_user (works even without shared server)
    try:
        target_user = await bot.fetch_user(YOUR_DISCORD_USER_ID)
        print(
            f"✅ Successfully found user: {target_user.name}#{target_user.discriminator}"
        )
    except discord.errors.NotFound:
        print(
            f"❌ Could not find user with ID {YOUR_DISCORD_USER_ID}. Check if the ID is correct."
        )

    # Start the match checking task
    check_for_matches.start()


# Command to manually check for matches
@bot.command(name="checkvlr")
async def check_vlr(ctx):
    if ctx.author.id == YOUR_DISCORD_USER_ID:
        await ctx.send("Checking for new VLR matches...")
        new_completed_matches = scrape_vlr_matches()

        if new_completed_matches:
            await ctx.send(
                f"Found {len(new_completed_matches)} new completed matches!")
            for match_data in new_completed_matches:
                embed = create_match_embed(match_data)
                await ctx.send(content="🎮 New VLR.gg Match Completed!",
                               embed=embed)
        else:
            await ctx.send("No new completed matches found.")


# Task to periodically check for matches
@tasks.loop(minutes=60)  # Check every 60 minutes
async def check_for_matches():
    global target_user
    print("Checking for new VLR matches...")
    new_completed_matches = scrape_vlr_matches()

    if new_completed_matches:
        print(f"Found {len(new_completed_matches)} new completed matches!")

        # If we have the target user
        if target_user:
            try:
                for match_data in new_completed_matches:
                    embed = create_match_embed(match_data)
                    await target_user.send(
                        content="🎮 New VLR.gg Match Completed!", embed=embed)
                    print(
                        f"✅ Sent DM notification for {match_data[1]} vs {match_data[3]}"
                    )
            except discord.errors.Forbidden:
                print(
                    "❌ Cannot send DM to user - they have DMs disabled or blocked the bot"
                )
        else:
            print(
                f"❌ Target user not found. Still waiting to find user with ID {YOUR_DISCORD_USER_ID}"
            )
    else:
        print("No new completed matches found.")


# Function to create match embed
def create_match_embed(match_data):
    datetime_str, team1, score1, team2, score2, status, phase, tournament, match_link = match_data

    embed = discord.Embed(
        title=f"{team1} {score1} vs {score2} {team2}",
        url=match_link,
        description=f"Match has completed in {tournament}",
        color=0x7289DA  # Discord blue color
    )

    embed.add_field(name="Phase", value=phase, inline=True)
    embed.add_field(name="Status", value=status, inline=True)
    embed.add_field(name="Date", value=datetime_str, inline=True)

    return embed


# Simple command to test if DMs work
@bot.command(name="testdm")
async def test_dm(ctx):
    if ctx.author.id == YOUR_DISCORD_USER_ID:
        try:
            await ctx.author.send(
                "This is a test DM from the VLR Match Notifier bot!")
            await ctx.send(
                "✅ DM sent successfully! Check your direct messages.")
        except:
            await ctx.send(
                "❌ Could not send DM. Please check if you have DMs enabled for server members."
            )


if __name__ == "__main__":
    keep_alive()  # Start the Flask server to keep the repl alive
    bot.run(DISCORD_BOT_TOKEN)
