# Weather MCP Server

This directory contains a Model Context Protocol (MCP) server implementation for retrieving weather information.

## Features

### US Weather (NWS API)
- `get_alerts`: Retrieve weather alerts for US states
- `get_forecast`: Get weather forecast by latitude and longitude

### International Weather (OpenWeatherMap API)
- `get_current_weather`: Get current weather for any international city
- `get_forecast`: Get a 5-day forecast for any international city

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. The server is configured in the `.vscode/mcp.json` file:
   - US weather uses the NWS API (no API key needed)
   - International weather uses OpenWeatherMap API (API key included for demo purposes)

## Usage

After configuring the MCP server, you can ask questions like:
- "What's the weather in Hanoi, Vietnam?"
- "Get a weather forecast for Tokyo, Japan"
- "What are the weather alerts in CA?"

The appropriate server will handle the request based on the location.
