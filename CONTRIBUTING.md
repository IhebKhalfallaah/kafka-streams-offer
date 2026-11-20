# Contributing

Thanks for your interest in improving this project!

## Getting started

1. Fork the repository and create a branch from `main`:
   `git checkout -b feature/short-description`
2. Start the full stack to check your change end to end:
   ```bash
   cd docker
   docker compose up --build
   ```
3. Make sure each component still builds:
   - `streams-app`: `mvn package`
   - `backend-sse`: `./mvnw package`
   - `web`: `npm install && npm run build`

## Pull requests

- Keep each pull request focused on one change.
- Describe **what** changed and **why** in the PR description.
- For larger changes (new topics, scoring rules, message formats), please open
  an issue first so we can agree on the approach.

## Reporting issues

Open a GitHub issue with steps to reproduce, what you expected, and what happened
(include logs from `docker compose logs <service>` when relevant).
