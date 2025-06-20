# Colorado GMU Elk Hunting Success Prediction System

## Project Overview
Build a data-driven system to predict elk archery hunting success rates across Colorado Game Management Units (GMUs) by integrating herd statistics, harvest data, and weather patterns.

## Core Problem Statement
**Primary Question**: "Which GMU has the highest probability/success rate for over-the-counter elk archery tags based on herd statistics and recent weather patterns?"

## Project Scope (MVP)
- **Species**: Elk only
- **Season**: Archery season only  
- **Geographic**: Colorado GMUs and Data Analysis Units (DAUs)
- **Time Range**: Historical data with yearly updates
- **Storage**: Local DuckDB instance

## Data Sources

### 1. GMU Geographic Data
- **Source**: https://geodata.colorado.gov/datasets/2c0794ece2ee4c8d9ac1f64cda8d0216_0/about
- **Format**: GIS/Shapefile via API
- **Content**: GMU boundaries, geographic metadata

### 2. Elk Statistics (Primary Data)
- **Source**: https://cpw.state.co.us/hunting/big-game/elk/statistics
- **Format**: Multiple PDF files (harvest/population reports)
- **Example**: https://cpw.widencollective.com/assets/share/asset/wkisb2j1f4
- **Content**: Yearly harvest data, herd populations, success rates

### 3. Weather Data (Supporting)
- **Source**: National Weather Service APIs
- **Purpose**: Recent weather patterns affecting GMU/DAU areas
- **Update Frequency**: More frequent than yearly

## Technical Stack
- **Language**: Python
- **Database**: DuckDB (local storage)
- **PDF Processing**: Extract tabular data from CPW reports
- **GIS Processing**: Handle spatial GMU data
- **APIs**: FastAPI for technical users
- **NL Interface**: MCP server with MindsDB for natural language queries

## Key Technical Requirements

### Data Processing Pipeline
1. **Extract**: PDF parsing, GIS data ingestion, weather API calls
2. **Transform**: Clean, normalize, and structure data
3. **Load**: Store in optimized DuckDB schema
4. **Update**: Handle yearly data refreshes and weather updates

### Data Model Design
- Support yearly-based hunting statistics
- Spatial relationships between GMUs/DAUs
- Weather pattern correlation with hunting success
- Flexible schema for future species expansion

### Output Interfaces
1. **MCP Server**: Natural language queries ("Best GMU for elk archery?")
2. **REST API**: Programmatic access for technical users

## Success Metrics
- Accurate success rate predictions by GMU
- Integration of 3+ years historical data
- Sub-second query response times
- Clean API documentation and examples

## Development Phases
1. **Phase 1**: Data collection and basic ETL pipeline
2. **Phase 2**: Core data model and DuckDB schema
3. **Phase 3**: Analysis engine and success rate calculations
4. **Phase 4**: API and MCP server implementation
5. **Phase 5**: Weather integration and enhanced predictions

## Important Notes for Implementation
- **Chain of Thought**: Use sequential-thinking MCP for better reasoning during development
- **Context Management**: Use memory-bank MCP to track progress across sessions
- **PDF Complexity**: CPW PDFs may have inconsistent formatting - robust parsing required
- **Data Quality**: Implement validation for statistical data consistency
- **Scalability**: Design for future multi-species expansion

## MCP Usage Instructions for Cursor IDE

### Memory Bank MCP Commands
Track project progress using this pattern:
```
/mcp memory-bank-mcp track_progress action="Project Setup" description="Created initial project structure and virtual environment"
```
Additional tracking commands will be determined as development progresses.

### Sequential Thinking MCP
For complex problem-solving, start prompts with:
"Use sequential-thinking MCP to break down [specific task] step by step"

### Recommended Session Workflow
1. Start each coding session: `/mcp memory-bank-mcp get_progress` to review previous work
2. After completing features: Track progress with appropriate action/description
3. Before major decisions: Use sequential-thinking MCP for planning
4. End of session: Summary track_progress with overall session accomplishments

## Repository Structure (Suggested)
```
bow-hunter-pipeline/
├── src/
│   ├── data_collection/     # PDF parsing, API clients
│   ├── data_processing/     # ETL pipeline
│   └── mcp/                 # MCP server implementation
├── data/
│   ├── raw/                 # Original PDFs, GIS files
│   ├── processed/           # Cleaned data
│   └── database/            # DuckDB files
└── tests/                   # Unit and integration tests
```

## Next Steps
1. Set up development environment with required Python packages
2. Create initial data collection scripts for GMU GIS data
3. Develop PDF parsing pipeline for CPW statistics
4. Design and implement core DuckDB schema
5. Build basic analysis engine for success rate calculations