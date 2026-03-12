# phase 1 functional requirements

## inputs
- `METABASE_BASE_URL`
- `METABASE_USERNAME`
- `METABASE_PASSWORD`
- `OUTPUT_DIR`

## required behavior
The phase 1 script must:

1. load configuration from `.env`
2. authenticate to Metabase with username and password
3. obtain a session token
4. use that token to call these API endpoints:
   - `/api/database`
   - `/api/collection`
   - `/api/dashboard`
   - `/api/card`
5. save each raw JSON response to disk
6. create output folders if they do not already exist
7. print simple status messages during execution
8. stop with a clear error if authentication fails
9. stop with a clear error if any API request fails

## required output files
- `output/raw/databases.json`
- `output/raw/collections.json`
- `output/raw/dashboards.json`
- `output/raw/cards.json`

## constraints
- keep code simple
- keep code readable
- no UI
- no database
- no recursive crawling yet
- no transformation beyond saving raw responses