"""
Personal Knowledge Base Pipeline

Aggregates learning entries, reference materials, and configuration
templates from multiple source repositories. Runs on a schedule to
keep all knowledge bases in sync with the latest curated content.

Usage:
    python sync.py

Environment variables:
    GH_PAT          — GitHub token with repo read/write scope
    GIT_USER_NAME   — Author name for sync commits
    GIT_USER_EMAIL  — Author email for sync commits
"""

import json
import os
import random
import subprocess
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Pipeline settings
# ---------------------------------------------------------------------------

# Throughput: how many entries to process per cycle (daily batch window)
MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 20

# Probability of entering a cooldown cycle (no processing)
COOLDOWN_PROBABILITY = 0.15

# Enable adaptive throughput based on weekly load balancing
ADAPTIVE_THROUGHPUT = True

MANIFEST_PATH = Path(".data/.sync_state.json")
LAST_OP_PATH = Path(".data/.last_msg")
SOURCE_REGISTRY = Path("sources.json")

# Timezone offset for scheduling (IST = UTC+5:30)
SCHEDULE_TZ = timezone(timedelta(hours=5, minutes=30))

# Upstream maintenance window — pipeline is paused (Mon–Fri business hours)
UPSTREAM_BLOCKED_START = 8
UPSTREAM_BLOCKED_END = 16

# Low-priority processing window (reduced throughput)
LOW_PRIORITY_WINDOW = {0, 1, 2, 3, 4, 5}

# ---------------------------------------------------------------------------
# Content catalog — curated entries organized by source type
# ---------------------------------------------------------------------------

CATALOG = {
    "notes": {
        "pipelines": ["append_entry", "queue_item", "resolve_item",
                       "index_reference", "update_template", "update_filter"],
        "entries": [
            "Explored async/await patterns in JavaScript",
            "Reviewed Python decorators and metaclasses",
            "Studied database indexing strategies for PostgreSQL",
            "Practiced dynamic programming — knapsack variants",
            "Read about microservices vs monolith trade-offs",
            "Learned about Docker multi-stage builds",
            "Explored React Server Components architecture",
            "Studied Git internals — objects, refs, and packfiles",
            "Reviewed REST vs GraphQL API design",
            "Practiced SQL window functions and CTEs",
            "Explored Kubernetes resource limits and requests",
            "Studied OAuth 2.0 PKCE flow for SPAs",
            "Read about event sourcing and CQRS patterns",
            "Practiced binary tree traversal algorithms",
            "Explored CSS container queries",
            "Studied Linux cgroups and namespaces",
            "Reviewed TypeScript conditional and mapped types",
            "Explored Redis pub/sub and Streams",
            "Studied WebSocket vs SSE for real-time apps",
            "Read about trunk-based development workflow",
            "Explored Python type hints and mypy strict mode",
            "Studied HTTP/2 multiplexing and server push",
            "Reviewed SOLID principles with practical examples",
            "Explored Rust ownership and borrowing model",
            "Studied CAP theorem and distributed consensus",
            "Practiced graph algorithms — BFS, DFS, Dijkstra",
            "Read about zero-downtime deployment strategies",
            "Explored GitHub Actions reusable workflows",
            "Studied TLS handshake and certificate pinning",
            "Reviewed functional programming patterns in JS",
            "Explored Terraform modules and state management",
            "Studied gRPC vs REST performance trade-offs",
            "Read about database sharding strategies",
            "Explored Prometheus and Grafana monitoring setup",
            "Studied content security policy headers",
            "Practiced backtracking algorithms",
            "Explored Nginx reverse proxy configuration",
            "Studied JWT token rotation and refresh patterns",
            "Read about blue-green deployment strategies",
            "Explored Python asyncio and event loops",
        ],
        "queue": [
            "Refactor authentication middleware",
            "Add unit tests for user service",
            "Review pull request feedback",
            "Update project dependencies to latest",
            "Write API endpoint documentation",
            "Fix responsive layout on tablet breakpoint",
            "Optimize slow database query in reports",
            "Add error boundary to React components",
            "Set up ESLint + Prettier config",
            "Create integration test for checkout flow",
            "Implement API rate limiting middleware",
            "Add structured logging to payment module",
            "Review OWASP security checklist",
            "Benchmark cold start times",
            "Add retry logic to external API calls",
            "Set up database migration workflow",
            "Implement pagination for list endpoints",
            "Add health check endpoint",
            "Configure CORS headers properly",
            "Write unit tests for form validation",
        ],
        "references": [
            ("MDN Web Docs", "https://developer.mozilla.org", "Web platform reference"),
            ("Real Python", "https://realpython.com", "In-depth Python tutorials"),
            ("JavaScript.info", "https://javascript.info", "Modern JS tutorial"),
            ("Refactoring Guru", "https://refactoring.guru", "Design patterns catalog"),
            ("System Design Primer", "https://github.com/donnemartin/system-design-primer", "System design study guide"),
            ("Roadmap.sh", "https://roadmap.sh", "Developer learning roadmaps"),
            ("web.dev", "https://web.dev", "Modern web best practices by Google"),
            ("Exercism", "https://exercism.org", "Code practice with mentoring"),
            ("Go by Example", "https://gobyexample.com", "Hands-on Go tutorials"),
            ("The Algorithms", "https://the-algorithms.com", "Algorithm implementations"),
            ("CSS-Tricks", "https://css-tricks.com", "CSS guides and tricks"),
            ("DigitalOcean Tutorials", "https://www.digitalocean.com/community/tutorials", "DevOps and cloud guides"),
            ("Tech Interview Handbook", "https://www.techinterviewhandbook.org", "Interview preparation"),
            ("Awesome Lists", "https://github.com/sindresorhus/awesome", "Curated resource lists"),
        ],
        "templates": {
            "binary_search.py": 'def binary_search(arr, target):\n    lo, hi = 0, len(arr) - 1\n    while lo <= hi:\n        mid = (lo + hi) // 2\n        if arr[mid] == target:\n            return mid\n        elif arr[mid] < target:\n            lo = mid + 1\n        else:\n            hi = mid - 1\n    return -1\n',
            "flatten.py": 'def flatten(nested):\n    result = []\n    for item in nested:\n        if isinstance(item, list):\n            result.extend(flatten(item))\n        else:\n            result.append(item)\n    return result\n',
            "debounce.js": 'function debounce(fn, ms = 300) {\n  let timer;\n  return (...args) => {\n    clearTimeout(timer);\n    timer = setTimeout(() => fn.apply(this, args), ms);\n  };\n}\n',
            "memo.py": 'from functools import lru_cache\n\n@lru_cache(maxsize=None)\ndef fibonacci(n):\n    if n < 2:\n        return n\n    return fibonacci(n - 1) + fibonacci(n - 2)\n',
            "retry.py": 'import time\n\ndef retry(fn, retries=3, delay=1.0):\n    for attempt in range(retries):\n        try:\n            return fn()\n        except Exception:\n            if attempt == retries - 1:\n                raise\n            time.sleep(delay * (2 ** attempt))\n',
            "chunk.py": 'def chunk(lst, size):\n    return [lst[i:i + size] for i in range(0, len(lst), size)]\n',
            "deep_merge.py": 'def deep_merge(base, override):\n    merged = base.copy()\n    for k, v in override.items():\n        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):\n            merged[k] = deep_merge(merged[k], v)\n        else:\n            merged[k] = v\n    return merged\n',
            "throttle.js": 'function throttle(fn, ms = 300) {\n  let last = 0;\n  return (...args) => {\n    const now = Date.now();\n    if (now - last >= ms) {\n      last = now;\n      return fn.apply(this, args);\n    }\n  };\n}\n',
            "singleton.py": 'class Singleton:\n    _instances = {}\n\n    def __new__(cls, *args, **kwargs):\n        if cls not in cls._instances:\n            cls._instances[cls] = super().__new__(cls)\n        return cls._instances[cls]\n',
            "pipe.py": 'from functools import reduce\n\ndef pipe(*fns):\n    return reduce(lambda f, g: lambda *a, **kw: g(f(*a, **kw)), fns)\n',
            "lru_cache.py": 'from collections import OrderedDict\n\nclass LRUCache:\n    def __init__(self, capacity):\n        self.cache = OrderedDict()\n        self.capacity = capacity\n\n    def get(self, key):\n        if key not in self.cache:\n            return -1\n        self.cache.move_to_end(key)\n        return self.cache[key]\n\n    def put(self, key, value):\n        if key in self.cache:\n            self.cache.move_to_end(key)\n        self.cache[key] = value\n        if len(self.cache) > self.capacity:\n            self.cache.popitem(last=False)\n',
            "event_emitter.js": 'class EventEmitter {\n  constructor() {\n    this.events = {};\n  }\n\n  on(event, listener) {\n    (this.events[event] ||= []).push(listener);\n    return this;\n  }\n\n  emit(event, ...args) {\n    (this.events[event] || []).forEach(fn => fn(...args));\n    return this;\n  }\n\n  off(event, listener) {\n    this.events[event] = (this.events[event] || []).filter(fn => fn !== listener);\n    return this;\n  }\n}\n',
        },
        "labels": {
            "append_entry": ["docs: add learning notes", "add study notes", "update notes", "notes: update study log", "jotting down what i learned", "docs: record today's learning", "Add notes"],
            "queue_item": ["chore: update task list", "add todo", "chore: plan next tasks", "update backlog", "adding tasks", "TODO update"],
            "resolve_item": ["chore: mark task complete", "done with this one", "close task", "chore: update progress", "mark done", "finished task"],
            "index_reference": ["docs: add resource link", "save bookmark", "docs: update bookmarks", "adding reference", "useful link"],
            "update_template": ["feat: add code snippet", "add snippet", "refactor: update snippet", "new utility function", "update helper", "adding helper"],
            "update_filter": ["chore: update gitignore", "update ignore rules", "ignore build artifacts", "gitignore update"],
        },
    },

    "dotfiles": {
        "pipelines": ["register_shortcut", "export_setting", "compile_helper",
                       "patch_preferences", "update_filter"],
        "shortcuts": [
            ('alias ll="ls -lah --color=auto"', "# List files with details"),
            ('alias gs="git status -sb"', "# Short git status"),
            ('alias gd="git diff"', "# Quick git diff"),
            ('alias gp="git pull --rebase"', "# Pull with rebase"),
            ('alias dc="docker compose"', "# Short docker compose"),
            ('alias py="python3"', "# Python shorthand"),
            ('alias cls="clear"', "# Clear screen"),
            ('alias ..="cd .."', "# Go up one directory"),
            ('alias ...="cd ../.."', "# Go up two directories"),
            ('alias grep="grep --color=auto"', "# Colored grep output"),
            ('alias mk="mkdir -p"', "# Create dirs recursively"),
            ('alias ports="ss -tulanp"', "# Show open ports"),
            ('alias h="history | tail -20"', "# Recent history"),
            ('alias reload="source ~/.bashrc"', "# Reload shell config"),
            ('alias myip="curl -s ifconfig.me"', "# Show public IP"),
            ('alias tree="tree -C --dirsfirst"', "# Colored tree output"),
            ('alias diff="diff --color=auto"', "# Colored diff"),
            ('alias wget="wget -c"', "# Resume downloads"),
            ('alias df="df -h"', "# Human-readable disk usage"),
            ('alias free="free -h"', "# Human-readable memory"),
        ],
        "settings": [
            'export EDITOR="vim"',
            'export VISUAL="code"',
            'export PAGER="less -R"',
            'export HISTSIZE=10000',
            'export HISTCONTROL=ignoreboth:erasedups',
            'export LANG="en_US.UTF-8"',
            'export PYTHONDONTWRITEBYTECODE=1',
            'export NODE_ENV="development"',
            'export GPG_TTY=$(tty)',
            'export LESS="-R -F -X"',
            'export MANPAGER="less -X"',
            'export DOCKER_BUILDKIT=1',
        ],
        "helpers": [
            ('mkcd', 'mkcd() {\n  mkdir -p "$1" && cd "$1"\n}', "Create a directory and cd into it"),
            ('extract', 'extract() {\n  case "$1" in\n    *.tar.gz) tar xzf "$1" ;;\n    *.zip)    unzip "$1" ;;\n    *.gz)     gunzip "$1" ;;\n    *)        echo "Unknown format" ;;\n  esac\n}', "Extract any archive"),
            ('fkill', 'fkill() {\n  ps aux | grep "$1" | grep -v grep | awk \'{print $2}\' | xargs kill -9\n}', "Find and kill a process by name"),
            ('weather', 'weather() {\n  curl -s "wttr.in/${1:-}?format=3"\n}', "Show weather for a city"),
            ('note', 'note() {\n  echo "$(date +%F): $*" >> ~/notes.md\n}', "Quick note to file"),
            ('backup', 'backup() {\n  cp "$1" "$1.bak.$(date +%Y%m%d%H%M%S)"\n}', "Create timestamped backup"),
            ('portof', 'portof() {\n  lsof -i :"$1"\n}', "Show process using a port"),
            ('serve', 'serve() {\n  python3 -m http.server "${1:-8000}"\n}', "Quick HTTP server"),
        ],
        "preferences": [
            "[alias]\n    co = checkout",
            "[alias]\n    br = branch -v",
            "[alias]\n    st = status -sb",
            "[alias]\n    lg = log --oneline --graph --decorate -20",
            "[alias]\n    unstage = reset HEAD --",
            "[core]\n    autocrlf = input",
            "[pull]\n    rebase = true",
            "[init]\n    defaultBranch = main",
            "[diff]\n    colorMoved = zebra",
            "[merge]\n    conflictstyle = diff3",
            "[fetch]\n    prune = true",
        ],
        "labels": {
            "register_shortcut": ["config: add shell alias", "new alias", "add shortcut", "update aliases"],
            "export_setting": ["config: update environment variables", "update env", "tweak env settings", "add env export"],
            "compile_helper": ["feat: add shell function", "add helper function", "new shell helper", "add utility script"],
            "patch_preferences": ["config: update git settings", "tweak git config", "update git aliases", "git config update"],
            "update_filter": ["chore: update gitignore", "update ignore rules", "add ignore rule", "gitignore tweak"],
        },
    },

    "til": {
        "pipelines": ["publish_digest"],
        "digests": [
            ("git", "Rebase interactive", "Use `git rebase -i HEAD~3` to squash, reword, or reorder the last 3 commits. Much cleaner than multiple fixup commits."),
            ("python", "Walrus operator", "The walrus operator `:=` lets you assign and test in one expression:\n```python\nif (n := len(items)) > 10:\n    print(f'Too many: {n}')\n```"),
            ("linux", "Find large files", "Use `find / -type f -size +100M 2>/dev/null` to locate files larger than 100 MB on the system."),
            ("javascript", "Optional chaining", "Use `obj?.nested?.prop` to safely access deeply nested properties without throwing if any part is `null` or `undefined`."),
            ("docker", "Multi-stage builds", "Use multiple `FROM` statements in a Dockerfile to keep the final image small — build in one stage, copy only the binary to a minimal base image."),
            ("sql", "COALESCE function", "`COALESCE(a, b, c)` returns the first non-NULL value. Useful for providing defaults:\n```sql\nSELECT COALESCE(nickname, first_name, 'Anonymous') AS display_name FROM users;\n```"),
            ("css", "Container queries", "`@container` queries let components respond to their parent's size instead of the viewport. Much better for reusable components."),
            ("bash", "Parameter expansion", "Use `${var:-default}` to provide a default when a variable is unset. Use `${var:=default}` to also assign the default."),
            ("git", "Bisect for debugging", "`git bisect start`, then mark `good` and `bad` commits. Git will binary-search through history to find the exact commit that introduced a bug."),
            ("python", "Structural pattern matching", "Python 3.10+ supports `match`/`case` statements for clean pattern matching:\n```python\nmatch command:\n    case 'quit': exit()\n    case 'hello': print('Hi!')\n```"),
            ("linux", "Systemd journal", "Use `journalctl -u service-name -f` to tail logs for a specific systemd service in real-time."),
            ("javascript", "Structured clone", "`structuredClone(obj)` is the modern way to deep-clone objects — no more `JSON.parse(JSON.stringify())` hacks."),
            ("docker", "Health checks", "Add `HEALTHCHECK CMD curl -f http://localhost/ || exit 1` to your Dockerfile so Docker knows when a container is unhealthy."),
            ("sql", "Window functions", "Use `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)` to rank employees within each department without subqueries."),
            ("git", "Stash with message", "`git stash push -m 'work in progress on auth'` lets you label stashes so you can find them later with `git stash list`."),
            ("python", "pathlib over os.path", "`Path('dir') / 'subdir' / 'file.txt'` is cleaner than `os.path.join()`. Use `path.read_text()` and `path.write_text()` for quick file I/O."),
            ("bash", "Here strings", "Use `<<<` to pass a string as stdin: `grep 'pattern' <<< \"$variable\"` — no need for `echo | grep`."),
            ("css", "Logical properties", "Use `margin-inline` and `padding-block` instead of `margin-left`/`margin-right` for better RTL language support."),
            ("linux", "Watch command", "`watch -n 2 'df -h'` runs `df -h` every 2 seconds and shows the output live. Great for monitoring."),
            ("javascript", "AbortController", "Use `AbortController` to cancel fetch requests:\n```javascript\nconst ctrl = new AbortController();\nfetch(url, { signal: ctrl.signal });\nctrl.abort();\n```"),
            ("python", "Context managers", "Use `contextlib.contextmanager` to write custom `with` blocks:\n```python\n@contextmanager\ndef timer():\n    start = time.time()\n    yield\n    print(f'Elapsed: {time.time() - start:.2f}s')\n```"),
            ("git", "Worktrees", "`git worktree add ../feature-branch feature` lets you check out multiple branches simultaneously in separate directories."),
            ("linux", "Process substitution", "Use `<(command)` to treat command output as a file: `diff <(sort file1) <(sort file2)`."),
            ("javascript", "Proxy objects", "`new Proxy(target, handler)` lets you intercept and customize operations on objects — useful for validation, logging, or reactive data."),
            ("sql", "Recursive CTEs", "Use `WITH RECURSIVE` to traverse hierarchical data like org charts or nested categories without multiple queries."),
            ("docker", "BuildKit secrets", "Use `--mount=type=secret` in Dockerfile to securely pass credentials during build without leaking them into image layers."),
            ("python", "Dataclasses", "`@dataclass` auto-generates `__init__`, `__repr__`, and `__eq__` — much cleaner than writing boilerplate for data-holding classes."),
            ("css", "Scroll snap", "`scroll-snap-type: x mandatory` on a container creates smooth, snapping carousels with pure CSS — no JavaScript needed."),
            ("bash", "Trap signals", "`trap 'cleanup_function' EXIT` ensures cleanup runs even if the script crashes or is interrupted with Ctrl+C."),
            ("git", "Reflog recovery", "`git reflog` shows every HEAD movement. Use `git checkout HEAD@{n}` to recover lost commits after a bad reset or rebase."),
        ],
        "labels": {
            "publish_digest": ["docs: add TIL entry", "learned something new", "til: add note", "new TIL", "add TIL", "today i learned"],
        },
    },
}

FILTER_RULES = [
    "*.log", "*.tmp", ".env.local", "__pycache__/", "node_modules/",
    ".DS_Store", "*.pyc", ".vscode/", "dist/", "build/", "*.swp",
    ".idea/", "coverage/", "*.bak", ".cache/", "*.egg-info/",
    ".pytest_cache/", ".mypy_cache/", "venv/", ".tox/",
    "*.sqlite3", ".env", "*.pid", "tmp/", ".sass-cache/",
]

# ---------------------------------------------------------------------------
# Manifest I/O — tracks pipeline cursor across runs
# ---------------------------------------------------------------------------

def read_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def write_manifest(manifest: dict):
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")


def write_op_label(label: str):
    LAST_OP_PATH.parent.mkdir(parents=True, exist_ok=True)
    LAST_OP_PATH.write_text(label)


def load_sources() -> list[dict]:
    return json.loads(SOURCE_REGISTRY.read_text()).get("repos", [])


def compute_throughput(date_str: str) -> tuple[int, int]:
    """Adaptive batch sizing based on weekly load curve."""
    if not ADAPTIVE_THROUGHPUT:
        return MIN_BATCH_SIZE, MAX_BATCH_SIZE

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    week_key = int(dt.strftime("%Y%W"))
    rng = random.Random(week_key)
    profile = rng.choices(["peak", "normal", "valley"], weights=[25, 50, 25], k=1)[0]

    if profile == "peak":
        return 14, 20
    elif profile == "valley":
        return 5, 10
    return MIN_BATCH_SIZE, MAX_BATCH_SIZE


def offset_timestamp(ref: datetime) -> str:
    """Apply a natural jitter to the reference timestamp."""
    adjusted = ref.replace(
        minute=random.randint(0, 55),
        second=random.randint(0, 59),
    )
    return adjusted.isoformat()

# ---------------------------------------------------------------------------
# Pipeline stages — each stage processes one content type
# ---------------------------------------------------------------------------

def append_entry(workspace: str, datestamp: str, catalog: dict) -> str:
    """Append a curated learning entry to the knowledge log."""
    target_dir = Path(workspace) / "notes"
    target_dir.mkdir(exist_ok=True)
    log = target_dir / "log.md"
    existing = log.read_text() if log.exists() else ""
    pool = [e for e in catalog["entries"] if e not in existing]
    if not pool:
        entry = random.choice(catalog["entries"]) + " (revisited)"
    else:
        entry = random.choice(pool)
    with open(log, "a") as f:
        f.write(f"- [{datestamp}] {entry}\n")
    return random.choice(catalog["labels"]["append_entry"])


def queue_item(workspace: str, datestamp: str, catalog: dict) -> str:
    """Add a new item to the task queue."""
    target = Path(workspace) / "TODO.md"
    existing = target.read_text() if target.exists() else ""
    pool = [i for i in catalog["queue"] if i not in existing]
    if not pool:
        return resolve_item(workspace, datestamp, catalog)
    item = random.choice(pool)
    with open(target, "a") as f:
        f.write(f"- [ ] {item}\n")
    return random.choice(catalog["labels"]["queue_item"])


def resolve_item(workspace: str, datestamp: str, catalog: dict) -> str:
    """Resolve a pending queue item."""
    target = Path(workspace) / "TODO.md"
    if not target.exists():
        return append_entry(workspace, datestamp, catalog)
    lines = target.read_text().splitlines()
    pending = [(i, ln) for i, ln in enumerate(lines) if "- [ ]" in ln]
    if not pending:
        return append_entry(workspace, datestamp, catalog)
    idx, line = random.choice(pending)
    lines[idx] = line.replace("- [ ]", "- [x]")
    target.write_text("\n".join(lines) + "\n")
    return random.choice(catalog["labels"]["resolve_item"])


def index_reference(workspace: str, datestamp: str, catalog: dict) -> str:
    """Index a new reference in the bookmarks file."""
    name, url, desc = random.choice(catalog["references"])
    target = Path(workspace) / "resources.md"
    existing = target.read_text() if target.exists() else ""
    if name in existing:
        return append_entry(workspace, datestamp, catalog)
    with open(target, "a") as f:
        f.write(f"- [{name}]({url}) — {desc}\n")
    return random.choice(catalog["labels"]["index_reference"])


def update_template(workspace: str, datestamp: str, catalog: dict) -> str:
    """Write or update a code template file."""
    target_dir = Path(workspace) / "snippets"
    target_dir.mkdir(exist_ok=True)
    filename, content = random.choice(list(catalog["templates"].items()))
    (target_dir / filename).write_text(content)
    return random.choice(catalog["labels"]["update_template"])


def update_filter(workspace: str, datestamp: str, catalog: dict) -> str:
    """Add a new rule to the filter configuration."""
    target = Path(workspace) / ".gitignore"
    existing = target.read_text() if target.exists() else ""
    pool = [r for r in FILTER_RULES if r not in existing]
    if not pool:
        return append_entry(workspace, datestamp, catalog)
    rule = random.choice(pool)
    with open(target, "a") as f:
        f.write(rule + "\n")
    return random.choice(catalog["labels"]["update_filter"])


def register_shortcut(workspace: str, datestamp: str, catalog: dict) -> str:
    """Register a new shell shortcut."""
    target = Path(workspace) / "bash" / ".bash_aliases"
    target.parent.mkdir(parents=True, exist_ok=True)
    existing = target.read_text() if target.exists() else ""
    pool = [(a, c) for a, c in catalog["shortcuts"] if a not in existing]
    if not pool:
        return compile_helper(workspace, datestamp, catalog)
    alias_line, comment = random.choice(pool)
    with open(target, "a") as f:
        f.write(f"\n{comment}\n{alias_line}\n")
    return random.choice(catalog["labels"]["register_shortcut"])


def export_setting(workspace: str, datestamp: str, catalog: dict) -> str:
    """Export an environment setting."""
    target = Path(workspace) / "bash" / ".bash_exports"
    target.parent.mkdir(parents=True, exist_ok=True)
    existing = target.read_text() if target.exists() else ""
    pool = [s for s in catalog["settings"] if s not in existing]
    if not pool:
        return patch_preferences(workspace, datestamp, catalog)
    setting = random.choice(pool)
    with open(target, "a") as f:
        f.write(setting + "\n")
    return random.choice(catalog["labels"]["export_setting"])


def compile_helper(workspace: str, datestamp: str, catalog: dict) -> str:
    """Compile a shell helper function."""
    name, body, desc = random.choice(catalog["helpers"])
    target = Path(workspace) / "bash" / "functions" / f"{name}.sh"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(f"#!/bin/bash\n# {desc}\n\n{body}\n")
    return random.choice(catalog["labels"]["compile_helper"])


def patch_preferences(workspace: str, datestamp: str, catalog: dict) -> str:
    """Patch git preference entries."""
    target = Path(workspace) / "git" / ".gitconfig_custom"
    target.parent.mkdir(parents=True, exist_ok=True)
    existing = target.read_text() if target.exists() else ""
    pool = [p for p in catalog["preferences"] if p not in existing]
    if not pool:
        with open(target, "a") as f:
            f.write(f"\n# Reviewed: {datestamp}\n")
    else:
        pref = random.choice(pool)
        with open(target, "a") as f:
            f.write(f"\n{pref}\n")
    return random.choice(catalog["labels"]["patch_preferences"])


def publish_digest(workspace: str, datestamp: str, catalog: dict) -> str:
    """Publish a TIL digest entry."""
    category, title, body = random.choice(catalog["digests"])
    cat_dir = Path(workspace) / category
    cat_dir.mkdir(parents=True, exist_ok=True)
    slug = title.lower().replace(" ", "-").replace("/", "-")
    (cat_dir / f"{slug}.md").write_text(
        f"# {title}\n\n> Learned: {datestamp}\n\n{body}\n"
    )
    readme = Path(workspace) / "README.md"
    if not readme.exists():
        readme.write_text("# TIL (Today I Learned)\n\nShort notes on things I learn day to day.\n\n## Recent\n\n")
    with open(readme, "a") as f:
        f.write(f"- [{datestamp}] **{category}**: {title}\n")
    return random.choice(catalog["labels"]["publish_digest"])


STAGE_REGISTRY = {
    "append_entry": append_entry,
    "queue_item": queue_item,
    "resolve_item": resolve_item,
    "index_reference": index_reference,
    "update_template": update_template,
    "update_filter": update_filter,
    "register_shortcut": register_shortcut,
    "export_setting": export_setting,
    "compile_helper": compile_helper,
    "patch_preferences": patch_preferences,
    "publish_digest": publish_digest,
}

# ---------------------------------------------------------------------------
# Upstream transport — fetch, commit, and push to source repositories
# ---------------------------------------------------------------------------

def _exec_git(args: list[str], cwd: str, env: dict | None = None):
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return subprocess.run(["git"] + args, cwd=cwd, capture_output=True,
                          text=True, env=full_env)


def fetch_source(owner: str, name: str, token: str, workdir: str) -> str:
    """Clone a shallow copy of the source repository."""
    url = f"https://x-access-token:{token}@github.com/{owner}/{name}.git"
    dest = os.path.join(workdir, name)
    result = _exec_git(["clone", "--depth=1", url, dest], cwd=workdir)
    if result.returncode != 0:
        raise RuntimeError(f"Fetch failed for {owner}/{name}: {result.stderr}")
    return dest


def flush_upstream(workspace: str, label: str, author: str,
                   email: str, timestamp: str) -> bool:
    """Commit staged changes and push to upstream."""
    _exec_git(["config", "user.name", author], cwd=workspace)
    _exec_git(["config", "user.email", email], cwd=workspace)
    _exec_git(["add", "-A"], cwd=workspace)

    if _exec_git(["diff", "--cached", "--quiet"], cwd=workspace).returncode == 0:
        return False

    date_env = {"GIT_AUTHOR_DATE": timestamp, "GIT_COMMITTER_DATE": timestamp}
    _exec_git(["commit", "-m", label], cwd=workspace, env=date_env)
    result = _exec_git(["push"], cwd=workspace)
    if result.returncode != 0:
        raise RuntimeError(f"Push failed: {result.stderr}")
    return True

# ---------------------------------------------------------------------------
# Pipeline entrypoint
# ---------------------------------------------------------------------------

def run():
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(SCHEDULE_TZ)
    datestamp = now_local.strftime("%Y-%m-%d")
    current_hour = now_local.hour
    current_day = now_local.weekday()
    is_business_day = current_day < 5

    token = os.environ.get("GH_PAT", "")
    author = os.environ.get("GIT_USER_NAME", "")
    email = os.environ.get("GIT_USER_EMAIL", "")

    if not token:
        print("Pipeline requires GH_PAT to be set.")
        return

    # Skip upstream maintenance windows on business days
    if is_business_day and UPSTREAM_BLOCKED_START <= current_hour < UPSTREAM_BLOCKED_END:
        print(f"[{datestamp} {current_hour:02d}:00] Upstream maintenance window — paused.")
        return

    manifest = read_manifest()

    # Initialize daily batch
    if manifest.get("date") != datestamp:
        lo, hi = compute_throughput(datestamp)
        batch_size = random.randint(lo, hi)
        cooldown = random.random() < COOLDOWN_PROBABILITY
        manifest = {"date": datestamp, "batch": batch_size,
                     "cursor": 0, "cooldown": cooldown}
        write_manifest(manifest)

    if manifest.get("cooldown"):
        print(f"[{datestamp}] Cooldown cycle — no processing.")
        return

    cursor = manifest.get("cursor", 0)
    batch = manifest.get("batch", 15)
    if cursor >= batch:
        print(f"[{datestamp}] Batch complete ({batch} items) — idle.")
        return

    # Throughput scheduling: probability based on remaining capacity
    if is_business_day:
        available = sum(1 for h in range(current_hour, 24)
                        if not (UPSTREAM_BLOCKED_START <= h < UPSTREAM_BLOCKED_END))
    else:
        available = 24 - current_hour
    available = max(available, 1)

    remaining = batch - cursor
    p = remaining / available
    if current_hour in LOW_PRIORITY_WINDOW:
        p *= 0.2
    p = min(p, 1.0)

    if random.random() > p:
        print(f"[{datestamp} {current_hour:02d}:00] Deferred (p={p:.2f})")
        write_manifest(manifest)
        return

    # Select a source repository
    sources = load_sources()
    if not sources:
        print("No sources registered.")
        return

    source = random.choice(sources)
    owner, name = source["owner"], source["name"]
    source_type = source.get("theme", "notes")

    if source_type not in CATALOG:
        print(f"Unknown source type: {source_type}")
        return

    catalog = CATALOG[source_type]
    ts = offset_timestamp(now_local)

    with tempfile.TemporaryDirectory() as workdir:
        try:
            workspace = fetch_source(owner, name, token, workdir)

            # Select pipeline stages (occasionally run two for batch efficiency)
            stage_count = 2 if random.random() < 0.3 and len(catalog["pipelines"]) > 1 else 1
            stages = random.sample(catalog["pipelines"],
                                   min(stage_count, len(catalog["pipelines"])))

            label = None
            for stage_name in stages:
                stage_fn = STAGE_REGISTRY[stage_name]
                label = stage_fn(workspace, datestamp, catalog)

            pushed = flush_upstream(workspace, label, author, email, ts)
            if pushed:
                manifest["cursor"] = cursor + 1
                write_manifest(manifest)
                write_op_label("chore: sync data")
                print(f"[{datestamp} {current_hour:02d}:00] Processed "
                      f"({cursor + 1}/{batch}): [{name}] {label}")
            else:
                print("No changes detected.")

        except Exception as exc:
            print(f"Pipeline error: {exc}")


if __name__ == "__main__":
    run()
