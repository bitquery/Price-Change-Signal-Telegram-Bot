
import re
import asyncio
import json
import logging
import aiohttp
import os
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.error import RetryAfter
from html import escape
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Your bot token from the BotFather
BOT_TOKEN = "BOT_TOKEN"

# OAuth Token from Bitquery
OAUTH_TOKEN = "OAUTH_TOKEN"

# Function to split long text into smaller parts
def split_text(text, max_length):
    return [text[i:i + max_length] for i in range(0, len(text), max_length)]

# Function to send a long message as multiple smaller messages
async def send_long_message(update: Update, context: ContextTypes.DEFAULT_TYPE, long_message, max_message_length=4000):
    message_parts = split_text(long_message, max_message_length)
    for part in message_parts:
        while True:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id, 
                    text=part,
                    parse_mode=ParseMode.HTML
                )
                break  # Break the loop if the message is sent successfully
            except RetryAfter as e:
                logging.warning(f"Flood control exceeded. Retrying in {e.retry_after} seconds.")
                await asyncio.sleep(e.retry_after)  # Wait for the specified time before retrying

# Function to send the query and process the response
async def send_query_and_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = 'https://streaming.bitquery.io/eap'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {OAUTH_TOKEN}'
    }
    query = """
    query MyQuery($time_5min_ago: DateTime, $time_1h_ago: DateTime) {
  Solana(network: solana) {
    DEXTradeByTokens(
      where: {Transaction: {Result: {Success: true}}, Block: {Time: {since: $time_1h_ago}}, any: [{Trade: {Side: {Currency: {MintAddress: {is: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}}}}}, {Trade: {Currency: {MintAddress: {not: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}}, Side: {Currency: {MintAddress: {is: "So11111111111111111111111111111111111111112"}}}}}, {Trade: {Currency: {MintAddress: {notIn: ["So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]}}, Side: {Currency: {MintAddress: {notIn: ["So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]}}}}}]}
      limit: {count: 25000}
      orderBy: {descendingByField: "traded_volume"}
    ) {
      Trade {
        Currency {
          Name
          MintAddress
          Symbol
        }
        start: Price(minimum: Block_Time)
        min5: Price(
          minimum: Block_Time
          if: {Block: {Time: {after: $time_5min_ago}}}
        )
        end: Price(maximum: Block_Time)
        current: PriceInUSD(maximum: Block_Time)
        Dex{
          ProtocolName
          ProtocolFamily
          ProgramAddress
        }
        Market{
          MarketAddress
        }
        Side {
          Currency {
            Symbol
            Name
            MintAddress
          }
        }
      }
      makers: count(distinct: Transaction_Signer)
      makers_5min: count(
        distinct: Transaction_Signer
        if: {Block: {Time: {after: $time_5min_ago}}}
      )
      buyers: count(
        distinct: Transaction_Signer
        if: {Trade: {Side: {Type: {is: buy}}}}
      )
      buyers_5min: count(
        distinct: Transaction_Signer
        if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time_5min_ago}}}
      )
      sellers: count(
        distinct: Transaction_Signer
        if: {Trade: {Side: {Type: {is: sell}}}}
      )
      sellers_5min: count(
        distinct: Transaction_Signer
        if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time_5min_ago}}}
      )
      trades: count
      trades_5min: count(if: {Block: {Time: {after: $time_5min_ago}}})
      traded_volume: sum(of: Trade_Side_AmountInUSD)
      traded_volume_5min: sum(
        of: Trade_Side_AmountInUSD
        if: {Block: {Time: {after: $time_5min_ago}}}
      )
      buy_volume: sum(
        of: Trade_Side_AmountInUSD
        if: {Trade: {Side: {Type: {is: buy}}}}
      )
      buy_volume_5min: sum(
        of: Trade_Side_AmountInUSD
        if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time_5min_ago}}}
      )
      sell_volume: sum(
        of: Trade_Side_AmountInUSD
        if: {Trade: {Side: {Type: {is: sell}}}}
      )
      sell_volume_5min: sum(
        of: Trade_Side_AmountInUSD
        if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time_5min_ago}}}
      )
      buys: count(if: {Trade: {Side: {Type: {is: buy}}}})
      buys_5min: count(
        if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time_5min_ago}}}
      )
      sells: count(if: {Trade: {Side: {Type: {is: sell}}}})
      sells_5min: count(
        if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time_5min_ago}}}
      )
    }
  }
}

    """
    now = datetime.now(timezone.utc)
    time_1h_ago = now - timedelta(hours=1)
    time_5min_ago = now - timedelta(minutes=5)
    print(time_1h_ago)
    print(time_5min_ago)

    variables = {
        "time_1h_ago": time_1h_ago.isoformat(),
        "time_5min_ago": time_5min_ago.isoformat()
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json={'query': query, 'variables': variables}) as response:
            response_text = await response.text()

            if response.status == 200:
                try:
                    response_json = json.loads(response_text)
                    solana_data = response_json.get('data', {}).get('Solana', {}).get('DEXTradeByTokens', [])
                    number_of_objects = len(solana_data)
                    print(number_of_objects)
                    
                    # Calculate price change for each item
                    for item in solana_data:
                        current_price = float(item['Trade'].get('end') or 0.0)
                        start_price = float(item['Trade'].get('start') or 0.0)
                        item['price_change_1hr'] = calculate_percentage_change(start_price, current_price)
                        
                    # Sort data by price change in descending order
                    sorted_data = sorted(
                        solana_data,
                        key=lambda x: float(re.sub(r'[^\d.-]', '', x['price_change_1hr']) or '0.0'),
                        reverse=True
                    )
                    formatted_message = format_message(sorted_data)
                    await send_long_message(update, context, formatted_message)
                except json.JSONDecodeError:
                    logging.error(f"Failed to parse JSON. Response: {response_text}")
                    await context.bot.send_message(chat_id=update.effective_chat.id, text="Failed to retrieve data.")
            else:
                logging.error(f"Request failed with status {response.status}")
                await context.bot.send_message(chat_id=update.effective_chat.id, text=f"API request failed with status code {response.status}.")


def calculate_percentage_change(start_price, end_price):
    """Calculate percentage change, return as formatted string with symbol."""
    try:
        # Ensure both prices are valid numbers and start_price is not zero
        if start_price != 0 and isinstance(start_price, (int, float)) and isinstance(end_price, (int, float)):
            change = ((end_price - start_price) / start_price) * 100
            symbol = "📈" if change > 0 else "📉" if change < 0 else ""
            return f"{change:.2f}% {symbol}"
        else:
            return "N/A"
    except (TypeError, ValueError):
        return "N/A"


def format_message(data):
    message = ""
    for item in data:
        try:
            # Trade details with enforced fallback for missing token names
            trade_currency_symbol = escape(item['Trade']['Currency'].get('Symbol') or "N/A")
            side_currency_symbol = escape(item['Trade']['Side']['Currency'].get('Symbol') or "N/A")
            trade_currency_mint = escape(item['Trade']['Currency'].get('MintAddress') or "N/A")
            side_currency_mint = escape(item['Trade']['Side']['Currency'].get('MintAddress') or "N/A")

            # Prices
            current_price = float(item['Trade'].get('end') or 0.0)
            start_price = float(item['Trade'].get('start') or 0.0)
            min5_price = float(item['Trade'].get('min5') or 0.0)
            current_usd_price= float(item['Trade'].get('current') or 0.0)

            # Calculated price changes
            price_change_5min = calculate_percentage_change(min5_price, current_price)
            price_change_1hr = calculate_percentage_change(start_price, current_price)

            # Trading statistics with 2 decimal places
            buy_volume = f"{float(item.get('buy_volume', 0)):.5f}" if item.get('buy_volume') else "N/A"
            sell_volume = f"{float(item.get('sell_volume', 0)):.5f}" if item.get('sell_volume') else "N/A"
            traded_volume = f"{float(item.get('traded_volume', 0)):.5f}" if item.get('traded_volume') else "N/A"
            buyers = str(item.get('buyers', "N/A"))
            sellers = str(item.get('sellers', "N/A"))
            buys = str(item.get('buys', "N/A"))
            sells = str(item.get('sells', "N/A"))
            trades = str(item.get('trades', "N/A"))
            makers = str(item.get('makers', "N/A"))

            # URL for Trade Now button
            trade_now_url = f"https://dexrabbit.com/solana/pair/{trade_currency_mint}/{side_currency_mint}"

            # Formatting the message part
            message_part = (
                f"<b><a href='https://dexrabbit.com/solana/token/{trade_currency_mint}'>{trade_currency_symbol}</a></b> | "
                f"<b><a href='https://dexrabbit.com/solana/token/{side_currency_mint}'>{side_currency_symbol}</a></b>\n"
                f"💲 <b>Current Price:</b> ${current_usd_price:.5f}\n"
                f"⏳ <b>5min</b> {price_change_5min}\n"
                f"⏳ <b>1hr</b> {price_change_1hr}\n\n"
                f"📈 <b>Buy Volume:</b> ${buy_volume} | 👥 <b>Buyers:</b> {buyers} | 🔼 <b>Buys:</b> {buys}\n"
                f"📉 <b>Sell Volume:</b> ${sell_volume} | 👥 <b>Sellers:</b> {sellers} | 🔽 <b>Sells:</b> {sells}\n"
                f"🔄 <b>Traded Volume:</b> ${traded_volume} | 📊 <b>Trades:</b> {trades}\n"
                f"👥 <b>Makers:</b> {makers}\n\n"
                f"<a href='{trade_now_url}'>💵 Trade Now</a>\n"
                "----------------------------------\n"
            )

            # Check if adding this message part will exceed Telegram's message length limit
            if (len(message) + len(message_part)) > 4096:
                logging.warning("Message length exceeded, consider sending the message in parts.")
                break  # Stop adding more content to avoid exceeding limits

            message += message_part
            
        except Exception as e:
            logging.error(f"Error formatting message for item: {item}. Error: {str(e)}")
            continue  # Skip this item if there's an error in formatting

    return message

# Add a global flag to prevent multiple tasks from running
is_task_running = False

async def start_regular_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global is_task_running
    if is_task_running:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Task already running.")
        return

    is_task_running = True  # Set the flag to indicate that the task is running
    try:
        while True:
            await send_query_and_process(update, context)
            await asyncio.sleep(1800)  # Wait for 30 minutes before sending the next request
    finally:
        is_task_running = False  # Ensure the flag is reset if the loop ends for any reason

# Command handler to start the regular requests
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Starting regular requests every 30 minutes...")
    asyncio.create_task(start_regular_requests(update, context))

# Main function to set up the Telegram bot
if __name__ == '__main__':
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    start_handler = CommandHandler('start', start)
    application.add_handler(start_handler)

    application.run_polling()
