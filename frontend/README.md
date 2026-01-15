# ModularData Frontend

React frontend for ModularData - AI-powered data transformation platform.

## Features

- Authentication with Supabase
- Dashboard with session management
- Interactive node graph for visualizing data transformations
- Chat interface for natural language data operations
- Data preview with transformation history

## Tech Stack

- React 19 + TypeScript
- Vite for bundling
- Tailwind CSS for styling
- React Flow for node graph visualization
- Supabase for authentication

## Development

```bash
# Install dependencies
npm install

# Create .env file with your credentials
cp .env.example .env
# Edit .env with your Supabase and API URLs

# Start dev server
npm run dev
```

## Environment Variables

Create a `.env` file with:

```
VITE_SUPABASE_URL=https://your-project.supabase.co
VITE_SUPABASE_ANON_KEY=your-anon-key
VITE_API_URL=https://your-api-url.railway.app
```

## Railway Deployment

1. Create a new service in Railway
2. Connect this directory as the root
3. Add the environment variables above as Railway variables
4. Railway will automatically build and deploy using nixpacks
