#!/usr/bin/env python3
"""Build the Acton package index from GitHub discovery."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any


GITHUB_API_URL = "https://api.github.com"
DEFAULT_LIBRARY_TOPIC = "acton-library"
DEFAULT_APP_TOPIC = "acton-app"
VALID_KINDS = {"library", "app"}
BUILD_NAME_RE = re.compile(r"^\s*name\s*=\s*[\"']([^\"']+)[\"']", re.MULTILINE)


class GitHubError(RuntimeError):
    pass


@dataclass
class Candidate:
    full_name: str
    html_url: str
    description: str | None = None
    default_branch: str | None = None
    stars: int = 0
    build_paths: set[str] = field(default_factory=set)
    kinds: set[str] = field(default_factory=set)


@dataclass
class Package:
    name: str
    kinds: set[str]
    description: str
    repo_url: str
    stars: int = 0
    discovered: bool = False


class GitHubClient:
    def __init__(self, token: str | None, verbose: bool = False) -> None:
        self.token = token
        self.verbose = verbose

    def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if path.startswith("https://"):
            url = path
        else:
            url = f"{GITHUB_API_URL}{path}"

        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"

        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "acton-package-index",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        if self.verbose:
            sys.stderr.write(f"GET {url}\n")

        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise GitHubError(
                f"GitHub API error {exc.code} for {url}: {body}"
            ) from exc
        except urllib.error.URLError as exc:
            raise GitHubError(f"GitHub API request failed for {url}: {exc}") from exc


def normalize_repo_url(url: str) -> str:
    normalized = url.strip()
    if normalized.endswith(".git"):
        normalized = normalized[:-4]
    return normalized.rstrip("/").lower()


def canonical_repo_url(full_name: str) -> str:
    return f"https://github.com/{full_name}"


def repo_path(full_name: str) -> str:
    return "/repos/" + urllib.parse.quote(full_name, safe="/")


def add_candidate(
    candidates: dict[str, Candidate],
    repo: dict[str, Any],
    kind: str,
    build_path: str | None = None,
) -> None:
    if kind not in VALID_KINDS:
        raise ValueError(f"unsupported package kind {kind!r}")

    full_name = repo.get("full_name")
    html_url = repo.get("html_url")
    if not full_name or not html_url:
        return

    key = full_name.lower()
    candidate = candidates.get(key)
    if candidate is None:
        candidate = Candidate(
            full_name=full_name,
            html_url=html_url,
            description=repo.get("description"),
            default_branch=repo.get("default_branch"),
            stars=int(repo.get("stargazers_count") or 0),
        )
        candidates[key] = candidate
    else:
        candidate.description = candidate.description or repo.get("description")
        candidate.default_branch = candidate.default_branch or repo.get("default_branch")
        candidate.stars = max(candidate.stars, int(repo.get("stargazers_count") or 0))

    if build_path:
        candidate.build_paths.add(build_path)
    candidate.kinds.add(kind)


def merge_candidates(
    candidates: dict[str, Candidate],
    additions: dict[str, Candidate],
) -> None:
    for key, addition in additions.items():
        candidate = candidates.get(key)
        if candidate is None:
            candidates[key] = addition
            continue

        candidate.description = candidate.description or addition.description
        candidate.default_branch = candidate.default_branch or addition.default_branch
        candidate.stars = max(candidate.stars, addition.stars)
        candidate.build_paths.update(addition.build_paths)
        candidate.kinds.update(addition.kinds)


def search_code(
    client: GitHubClient,
    query: str,
    per_page: int,
    max_pages: int,
    include_nested_build_files: bool,
    kind: str,
) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    for page in range(1, max_pages + 1):
        data = client.get_json(
            "/search/code",
            {
                "q": query,
                "per_page": per_page,
                "page": page,
                "sort": "indexed",
                "order": "desc",
            },
        )
        items = data.get("items", [])
        for item in items:
            path = item.get("path")
            if not include_nested_build_files and path not in {"Build.act", "build.act"}:
                continue
            add_candidate(candidates, item.get("repository") or {}, kind, path)

        if len(items) < per_page:
            break

    return candidates


def search_repositories(
    client: GitHubClient,
    query: str,
    per_page: int,
    max_pages: int,
    kind: str,
) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    for page in range(1, max_pages + 1):
        data = client.get_json(
            "/search/repositories",
            {"q": query, "per_page": per_page, "page": page},
        )
        items = data.get("items", [])
        for repo in items:
            add_candidate(candidates, repo, kind)

        if len(items) < per_page:
            break

    return candidates


def fetch_repo(client: GitHubClient, full_name: str) -> dict[str, Any]:
    return client.get_json(repo_path(full_name))


def fetch_build_file(
    client: GitHubClient,
    full_name: str,
    path: str,
    ref: str | None,
) -> str | None:
    params = {"ref": ref} if ref else None
    quoted_path = urllib.parse.quote(path, safe="/")
    try:
        data = client.get_json(f"{repo_path(full_name)}/contents/{quoted_path}", params)
    except GitHubError as exc:
        if "GitHub API error 404" in str(exc):
            return None
        raise

    if data.get("encoding") != "base64" or "content" not in data:
        return None

    raw = base64.b64decode(data["content"], validate=False)
    return raw.decode("utf-8", errors="replace")


def parse_build_name(build_file: str) -> str | None:
    match = BUILD_NAME_RE.search(build_file)
    if not match:
        return None
    return match.group(1).strip()


def normalize_package_name(name: str) -> str:
    package_name = name.strip().replace("_", "-").lower()
    if package_name.startswith("acton-"):
        package_name = package_name[len("acton-") :]
    return package_name


def package_from_candidate(
    client: GitHubClient,
    candidate: Candidate,
    include_archived: bool,
    include_forks: bool,
    verbose: bool,
) -> Package | None:
    if not candidate.kinds:
        if verbose:
            sys.stderr.write(f"Skipping {candidate.full_name}; no package kind\n")
        return None

    repo = fetch_repo(client, candidate.full_name)

    if repo.get("archived") and not include_archived:
        if verbose:
            sys.stderr.write(f"Skipping archived repository {candidate.full_name}\n")
        return None

    if repo.get("fork") and not include_forks:
        if verbose:
            sys.stderr.write(f"Skipping fork {candidate.full_name}\n")
        return None

    default_branch = repo.get("default_branch") or candidate.default_branch
    build_paths = sorted(candidate.build_paths) or ["Build.act"]
    build_file = None
    for build_path in build_paths:
        build_file = fetch_build_file(
            client,
            candidate.full_name,
            build_path,
            default_branch,
        )
        if build_file is not None:
            break

    if build_file is None:
        if verbose:
            sys.stderr.write(f"Skipping {candidate.full_name}; no Build.act found\n")
        return None

    build_name = parse_build_name(build_file)
    if not build_name:
        if verbose:
            sys.stderr.write(
                f"Skipping {candidate.full_name}; Build.act has no name field\n"
            )
        return None

    return Package(
        name=normalize_package_name(build_name),
        kinds=set(candidate.kinds),
        description=repo.get("description") or "",
        repo_url=canonical_repo_url(candidate.full_name),
        stars=int(repo.get("stargazers_count") or candidate.stars),
        discovered=True,
    )


def load_seed(path: str | None) -> list[Package]:
    if not path or not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    packages = []
    for item in data.get("packages", []):
        name = str(item.get("name") or "").strip()
        repo_url = str(item.get("repo_url") or "").strip()
        if not name or not repo_url:
            continue
        kinds = parse_kinds(item.get("kinds"), repo_url)
        packages.append(
            Package(
                name=name,
                kinds=kinds,
                description=str(item.get("description") or ""),
                repo_url=repo_url,
            )
        )
    return packages


def parse_kinds(value: Any, repo_url: str) -> set[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"package {repo_url} must have a non-empty 'kinds' list")

    kinds = set()
    for kind in value:
        if not isinstance(kind, str) or kind not in VALID_KINDS:
            raise ValueError(f"unsupported package kind {kind!r} for {repo_url}")
        kinds.add(kind)
    return kinds


def sorted_kinds(kinds: set[str]) -> list[str]:
    return [kind for kind in ("library", "app") if kind in kinds]


def merge_packages(
    seed: list[Package],
    discovered: list[Package],
    verbose: bool,
) -> list[Package]:
    by_repo = {normalize_repo_url(package.repo_url): package for package in seed}
    used_names = {
        (kind, package.name.lower())
        for package in seed
        for kind in package.kinds
    }

    for package in sorted(discovered, key=lambda item: (-item.stars, item.name, item.repo_url)):
        repo_key = normalize_repo_url(package.repo_url)
        existing = by_repo.get(repo_key)
        if existing:
            if not existing.description and package.description:
                existing.description = package.description
            existing.kinds.update(package.kinds)
            used_names.update((kind, existing.name.lower()) for kind in existing.kinds)
            continue

        name_keys = {(kind, package.name.lower()) for kind in package.kinds}
        if name_keys & used_names:
            if verbose:
                kinds = ", ".join(sorted_kinds(package.kinds))
                sys.stderr.write(
                    f"Skipping {package.repo_url}; package name "
                    f"{package.name!r} already exists for kinds: {kinds}\n"
                )
            continue

        by_repo[repo_key] = package
        used_names.update(name_keys)

    return sorted(
        by_repo.values(),
        key=lambda package: (package.name.lower(), package.repo_url),
    )


def write_index(path: str, packages: list[Package]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data = {
        "packages": [
            {
                "name": package.name,
                "kinds": sorted_kinds(package.kinds),
                "description": package.description,
                "repo_url": package.repo_url,
            }
            for package in packages
        ]
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")


def discover_packages(args: argparse.Namespace, token: str | None) -> list[Package]:
    client = GitHubClient(token, verbose=args.verbose)
    candidates: dict[str, Candidate] = {}

    for query in args.code_queries:
        if args.verbose:
            sys.stderr.write(f"Searching code: {query}\n")
        merge_candidates(
            candidates,
            search_code(
                client,
                query,
                args.per_page,
                args.max_pages,
                args.include_nested_build_files,
                args.query_kind,
            )
        )

    library_queries = list(args.library_repo_queries)
    library_queries.extend(f"topic:{topic}" for topic in args.library_topics)
    for query in library_queries:
        if args.verbose:
            sys.stderr.write(f"Searching library repositories: {query}\n")
        merge_candidates(
            candidates,
            search_repositories(client, query, args.per_page, args.max_pages, "library"),
        )

    app_queries = list(args.app_repo_queries)
    app_queries.extend(f"topic:{topic}" for topic in args.app_topics)
    for query in app_queries:
        if args.verbose:
            sys.stderr.write(f"Searching app repositories: {query}\n")
        merge_candidates(
            candidates,
            search_repositories(client, query, args.per_page, args.max_pages, "app"),
        )

    packages = []
    for candidate in sorted(candidates.values(), key=lambda item: item.full_name.lower()):
        package = package_from_candidate(
            client,
            candidate,
            include_archived=args.include_archived,
            include_forks=args.include_forks,
            verbose=args.verbose,
        )
        if package:
            packages.append(package)

    return packages


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build index.json by discovering Acton packages on GitHub."
    )
    parser.add_argument("--seed", default="index.json", help="Existing index to merge first.")
    parser.add_argument("-o", "--output", default="index.json", help="Index path to write.")
    parser.add_argument(
        "-q",
        "--query",
        action="append",
        dest="queries",
        help="GitHub code search query to add, such as 'filename:Build.act path:/'.",
    )
    parser.add_argument(
        "--no-code-search",
        action="store_true",
        help="Disable code search queries passed with --query.",
    )
    parser.add_argument(
        "--query-kind",
        choices=sorted(VALID_KINDS),
        default="library",
        help="Package kind to assign to repositories found by --query.",
    )
    parser.add_argument(
        "--library-topic",
        action="append",
        default=None,
        dest="library_topics",
        help=(
            "GitHub topic for library discovery. "
            f"Defaults to {DEFAULT_LIBRARY_TOPIC}."
        ),
    )
    parser.add_argument(
        "--app-topic",
        action="append",
        default=None,
        dest="app_topics",
        help=f"GitHub topic for app discovery. Defaults to {DEFAULT_APP_TOPIC}.",
    )
    parser.add_argument(
        "--no-topic-search",
        action="store_true",
        help="Disable default library and app topic discovery.",
    )
    parser.add_argument(
        "--library-repo-query",
        action="append",
        default=[],
        dest="library_repo_queries",
        help="Raw GitHub repository search query to add as library discovery.",
    )
    parser.add_argument(
        "--app-repo-query",
        action="append",
        default=[],
        dest="app_repo_queries",
        help="Raw GitHub repository search query to add as app discovery.",
    )
    parser.add_argument(
        "--github-token-env",
        default="GITHUB_TOKEN",
        help="Environment variable containing a GitHub token.",
    )
    parser.add_argument("--per-page", type=int, default=100, help="GitHub results per page.")
    parser.add_argument("--max-pages", type=int, default=10, help="Maximum pages per search.")
    parser.add_argument(
        "--include-nested-build-files",
        action="store_true",
        help="Include Build.act files below the repository root.",
    )
    parser.add_argument("--include-archived", action="store_true", help="Include archived repos.")
    parser.add_argument("--include-forks", action="store_true", help="Include forked repos.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Log discovery details.")
    args = parser.parse_args(argv)

    if args.per_page < 1 or args.per_page > 100:
        parser.error("--per-page must be between 1 and 100")
    if args.max_pages < 1:
        parser.error("--max-pages must be at least 1")

    args.code_queries = [] if args.no_code_search else (args.queries or [])
    if args.no_topic_search:
        args.library_topics = []
        args.app_topics = []
    else:
        args.library_topics = args.library_topics or [DEFAULT_LIBRARY_TOPIC]
        args.app_topics = args.app_topics or [DEFAULT_APP_TOPIC]

    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    token = os.environ.get(args.github_token_env)
    needs_github = bool(
        args.code_queries
        or args.library_repo_queries
        or args.app_repo_queries
        or args.library_topics
        or args.app_topics
    )
    if needs_github and not token:
        sys.stderr.write(f"Error: {args.github_token_env} is required for GitHub discovery\n")
        return 1

    try:
        seed = load_seed(args.seed)
        discovered = discover_packages(args, token) if needs_github else []
        packages = merge_packages(seed, discovered, args.verbose)
        write_index(args.output, packages)
    except (GitHubError, OSError, ValueError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"Error: {exc}\n")
        return 1

    if args.verbose:
        sys.stderr.write(
            f"Wrote {len(packages)} packages to {args.output} "
            f"({len(discovered)} discovered, {len(seed)} seeded)\n"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
