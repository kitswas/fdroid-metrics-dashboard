"""
Maps raw search query strings to package IDs.

Usage::

    mapper = QueryMapper()
    mapper.build_index(known_package_ids)          # required
    counts = mapper.map_query_hits(query_hits)     # {package_id: search_count}

The matching cascade (in priority order):
1. Exact package-id match
2. Exact normalized human name (from metadata cache)
3. Short-name equals normalized query (last dot-segment of package id)
4. Normalized query is a substring of, or equals, a normalized name
5. Fuzzy token-sort-ratio >= FUZZY_THRESHOLD (rapidfuzz or difflib fallback)

Queries that match nothing are silently discarded (search_count stays 0).
Queries that match multiple packages with similar scores are discarded to
avoid false attribution.
"""

from __future__ import annotations

import pathlib
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable

from rapidfuzz import fuzz as _rfuzz


def _fuzzy(a: str, b: str) -> int:
    return int(_rfuzz.token_sort_ratio(a, b))


def _load_yaml(path: pathlib.Path) -> dict:
    try:
        import yaml

        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except ImportError:
        return {}


ROOT = pathlib.Path(__file__).parent.parent
_METADATA_CACHE = ROOT / "cache" / "metadata"
_STOPWORDS: set[str] = {"app", "the", "a", "an"}

FUZZY_THRESHOLD = 90  # minimum score to auto-assign
AMBIGUITY_GAP = 5  # if 2nd-best >= top - gap AND top < 95, discard


# ---------------------------------------------------------------------------
# normalisation
# ---------------------------------------------------------------------------


def _normalize(text: str) -> str:
    """Lowercase, strip diacritics, remove punctuation, drop stopwords."""
    s = unicodedata.normalize("NFKD", text)
    s = s.casefold()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"_+", " ", s)
    s = " ".join(s.split())
    tokens = [t for t in s.split() if t not in _STOPWORDS]
    return " ".join(tokens)


# ---------------------------------------------------------------------------
# index entry
# ---------------------------------------------------------------------------


class _Entry:
    __slots__ = ("package_id", "norm_names", "tokens")

    def __init__(self, package_id: str) -> None:
        self.package_id = package_id
        self.norm_names: set[str] = set()
        self.tokens: set[str] = set()

    def add_name(self, name: str) -> None:
        norm = _normalize(name)
        if norm:
            self.norm_names.add(norm)
            self.tokens.update(norm.split())


# ---------------------------------------------------------------------------
# main class
# ---------------------------------------------------------------------------


class QueryMapper:
    """
    Maps raw search query strings to F-Droid package IDs.

    Call :meth:`build_index` once with the set of known package IDs before
    calling :meth:`map_query_hits`.
    """

    def __init__(self, metadata_dir: pathlib.Path | None = None) -> None:
        self._metadata_dir = metadata_dir or _METADATA_CACHE
        self._entries: dict[str, _Entry] = {}
        self._inv: dict[str, list[str]] = {}  # token → [package_id, ...]

    # ------------------------------------------------------------------
    # private helpers
    # ------------------------------------------------------------------

    def _make_entry(self, pkg: str) -> _Entry:
        entry = _Entry(pkg)
        entry.add_name(pkg)
        entry.add_name(pkg.split(".")[-1])
        return entry

    def _enrich_from_metadata(self) -> None:
        if not self._metadata_dir.exists():
            return
        for yml_path in self._metadata_dir.glob("*.yml"):
            pkg = yml_path.stem
            if pkg not in self._entries:
                self._entries[pkg] = self._make_entry(pkg)
            entry = self._entries[pkg]
            meta = _load_yaml(yml_path)
            for key in ("Name", "AutoName", "Summary"):
                val = meta.get(key)
                if isinstance(val, str):
                    entry.add_name(val)

    def _build_inverted_index(self) -> None:
        inv: dict[str, list[str]] = defaultdict(list)
        for pkg, entry in self._entries.items():
            for tok in entry.tokens:
                inv[tok].append(pkg)
        self._inv = dict(inv)

    def _token_candidates(self, q_toks: set[str]) -> dict[str, int]:
        scores: dict[str, int] = {}
        for tok in q_toks:
            for pkg in self._inv.get(tok, []):
                scores[pkg] = max(scores.get(pkg, 0), 80)
        return scores

    def _upgrade_scores(self, q_norm: str, scores: dict[str, int]) -> None:
        for pkg in scores:
            for nn in self._entries[pkg].norm_names:
                if nn and (nn in q_norm or q_norm in nn):
                    scores[pkg] = max(scores[pkg], 90)
        targets = scores if scores else self._entries
        for pkg in targets:
            best = max(
                (_fuzzy(q_norm, nn) for nn in self._entries[pkg].norm_names),
                default=0,
            )
            if best > scores.get(pkg, 0):
                scores[pkg] = best

    def _pick_winner(self, scores: dict[str, int]) -> str | None:
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_pkg, top_score = ranked[0]
        if top_score < FUZZY_THRESHOLD:
            return None
        if (
            len(ranked) > 1
            and ranked[1][1] >= top_score - AMBIGUITY_GAP
            and top_score < 95
        ):
            return None
        return top_pkg

    # ------------------------------------------------------------------
    # building the index
    # ------------------------------------------------------------------

    def build_index(self, package_ids: Iterable[str]) -> None:
        """
        Build the mapping index from a collection of known package IDs.

        Reads ``Name`` / ``AutoName`` from ``cache/metadata/*.yml`` for
        those packages that have a cached metadata file, and also registers
        the human-readable short name (last dot-segment of the package id).

        Args:
            package_ids: Iterable of package ID strings (e.g. from the app
                         metrics DataFrame).
        """
        self._entries.clear()
        self._inv.clear()

        for pkg in package_ids:
            pkg = str(pkg).strip()
            if pkg:
                self._entries[pkg] = self._make_entry(pkg)

        self._enrich_from_metadata()
        self._build_inverted_index()

    # ------------------------------------------------------------------
    # matching
    # ------------------------------------------------------------------

    def match(self, query: str) -> str | None:
        """
        Return the best-matching package ID for a query, or ``None``.

        Args:
            query: Raw search query string (e.g. ``"aurora store"``).

        Returns:
            Package ID string, or ``None`` if no confident match found.
        """
        if not self._entries:
            raise RuntimeError("Call build_index() before match().")

        q = query.strip()
        q_norm = _normalize(q)
        q_toks = set(q_norm.split()) if q_norm else set()

        # 1) exact package-id
        if q in self._entries:
            return q

        if not q_norm:
            return None

        # 2) exact normalized name
        for pkg, entry in self._entries.items():
            if q_norm in entry.norm_names:
                return pkg

        # 3-5) token + substring + fuzzy
        scores = self._token_candidates(q_toks)
        self._upgrade_scores(q_norm, scores)

        return self._pick_winner(scores) if scores else None

    # ------------------------------------------------------------------
    # bulk helper
    # ------------------------------------------------------------------

    def map_query_hits(self, query_hits: dict[str, int]) -> dict[str, int]:
        """
        Aggregate raw query hit counts into per-package search counts.

        A package may be matched by multiple queries (e.g. ``"aurora"`` and
        ``"aurora store"`` both map to ``com.aurora.store``); their hit
        counts are summed.

        Args:
            query_hits: Mapping of ``{query_string: hit_count}`` as returned
                        by :meth:`~etl.analyzer_search.SearchMetricsAnalyzer.get_query_analysis`.

        Returns:
            Mapping of ``{package_id: total_search_count}``.
        """
        result: dict[str, int] = defaultdict(int)
        for query, hits in query_hits.items():
            pkg = self.match(query)
            if pkg is not None:
                result[pkg] += int(hits)
        return dict(result)
