# <img src="app/static/img/logo.png" alt="Zygo" height="26"> Zygo

Zygo is a multi-tenant workflow automation platform specifically for InfoSec (Security) and IT teams.

![Zygo Flow Canvas](img/canvas.png)


![Zygo Dashboard](img/chart.png)


## What You Can Do

- **Build flows visually** — drag and drop nodes on a canvas to create automations without writing code
- **Connect to anything** — make HTTP requests to any API, receive webhooks, and process data in real time
- **Collect input** — publish web forms that trigger flows when submitted, with multi-step wizard support
- **Store and query data** — built-in data tables let your flows persist records across runs
- **Track work** — create tickets from flows for human review, approvals, and task tracking
- **Visualize results** — build dashboards with charts and stat cards from the data your flows collect

## Quick Start

1. Start the app with Docker
```commandline
git clone https://github.com/bmarsh9/zygo.git && cd zygo
docker compose up -d --build
```

2. Visit http://localhost:8000

3. Use `admin@example.com` and `admin1234567` to login as super user

4. [Create your first flow!](https://darkbanner.mintlify.app/quickstart)
