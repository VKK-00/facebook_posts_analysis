from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from social_posts_analysis.contracts import AuthorSnapshot, CommentSnapshot, PostSnapshot
from social_posts_analysis.utils import stable_id

from .facebook_web_timestamps import (
    extract_supported_date_hint_safe,
    normalize_mobile_text,
    parse_post_timestamp,
)


def build_comment_snapshots(
    *,
    post_id: str,
    payload_comments: list[dict[str, Any]],
    raw_path: str,
    source_collector: str,
) -> list[CommentSnapshot]:
    comments: list[CommentSnapshot] = []
    nesting_stack: list[dict[str, Any]] = []
    for payload_comment in payload_comments:
        raw_comment_text = payload_comment.get("text") or ""
        author_name = select_comment_author(payload_comment.get("author_name"), raw_comment_text)
        published_hint = payload_comment.get("published_hint") or derive_comment_published_hint(raw_comment_text)
        comment_text = clean_comment_text(raw_comment_text, author_name or "", published_hint)
        if len(comment_text) < 3:
            continue

        nesting_x = int(payload_comment.get("nesting_x") or 0)
        while nesting_stack and nesting_x <= int(nesting_stack[-1]["nesting_x"]) + 5:
            nesting_stack.pop()

        parent_comment_id = nesting_stack[-1]["comment_id"] if nesting_stack else None
        depth = len(nesting_stack)
        comment_id = stable_id(post_id, payload_comment.get("permalink") or comment_text[:160])
        snapshot = CommentSnapshot(
            comment_id=comment_id,
            platform="facebook",
            parent_post_id=post_id,
            parent_comment_id=parent_comment_id,
            thread_root_post_id=post_id,
            created_at=parse_post_timestamp(published_hint),
            message=comment_text,
            permalink=payload_comment.get("permalink"),
            reactions=0,
            source_collector=source_collector,
            depth=depth,
            raw_path=raw_path,
            author=AuthorSnapshot(author_id=None, name=author_name),
        )
        comments.append(snapshot)
        nesting_stack.append({"nesting_x": nesting_x, "comment_id": comment_id})
    return comments


def comment_article_limit(target_comment_count: int, aggressive: bool) -> int:
    base_limit = 220
    if aggressive:
        base_limit = 320
    if target_comment_count >= 80:
        return max(base_limit, 420 if aggressive else 280)
    if target_comment_count >= 30:
        return max(base_limit, 280 if aggressive else 240)
    return base_limit


def merge_extracted_comments(
    primary_comments: list[dict[str, Any]],
    fallback_comments: list[dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index_by_key: dict[str, int] = {}
    for comment in [*primary_comments, *fallback_comments]:
        normalized_comment = dict(comment)
        permalink = normalize_permalink(str(normalized_comment.get("permalink") or ""))
        if permalink:
            normalized_comment["permalink"] = permalink
        key = _comment_candidate_key(normalized_comment)
        existing_index = index_by_key.get(key)
        if existing_index is None:
            index_by_key[key] = len(merged)
            merged.append(normalized_comment)
            continue
        existing = merged[existing_index]
        if _comment_candidate_score(normalized_comment) > _comment_candidate_score(existing):
            merged[existing_index] = {
                **existing,
                **normalized_comment,
                "author_name": normalized_comment.get("author_name") or existing.get("author_name"),
                "published_hint": normalized_comment.get("published_hint") or existing.get("published_hint"),
                "permalink": normalized_comment.get("permalink") or existing.get("permalink"),
                "text": normalized_comment.get("text") or existing.get("text"),
            }
    if limit <= 0:
        return merged
    return merged[:limit]


def _comment_candidate_key(comment: dict[str, Any]) -> str:
    permalink = normalize_permalink(str(comment.get("permalink") or ""))
    if permalink:
        return f"permalink:{permalink}"
    normalized_text = normalize_mobile_text(str(comment.get("text") or ""))
    return f"text:{normalized_text[:200]}"


def _comment_candidate_score(comment: dict[str, Any]) -> int:
    score = len(normalize_mobile_text(str(comment.get("text") or "")))
    if comment.get("author_name"):
        score += 50
    if comment.get("published_hint"):
        score += 25
    if comment.get("permalink"):
        score += 25
    if comment.get("dom_parent_key"):
        score += 5
    return score


def comment_sort_menu_patterns() -> list[str]:
    return [
        r"\bMost relevant\b",
        r"\bTop comments\b",
        r"\bNewest\b",
        r"\bMost recent\b",
        r"\u041d\u0430\u0439\u0430\u043a\u0442\u0443\u0430\u043b\u044c\u043d\u0456\u0448\u0456\b",
        r"\u041d\u0430\u0439\u0430\u043a\u0442\u0443\u0430\u043b\u044c\u043d\u044b\u0435\b",
    ]


def comment_sort_option_patterns(*, aggressive: bool) -> list[str]:
    patterns = [
        r"\bAll comments\b",
        r"\bNewest\b",
        r"\bMost recent\b",
        r"\u0423\u0441\u0456 \u043a\u043e\u043c\u0435\u043d\u0442\u0430\u0440\u0456\b",
        r"\u0412\u0441\u0435 \u043a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0438\b",
    ]
    if aggressive:
        patterns.append(r"\bMost relevant\b")
    return patterns


def reel_comment_entry_patterns() -> list[str]:
    return [
        r"\bAll comments\b",
        r"\bComments?\b",
        r"\u0423\u0441\u0456 \u043a\u043e\u043c\u0435\u043d\u0442\u0430\u0440\u0456\b",
        r"\u0412\u0441\u0435 \u043a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0438\b",
        r"\u041a\u043e\u043c\u0435\u043d\u0442\u0430\u0440\u0456\b",
        r"\u041a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0438\b",
    ]


def comment_expansion_patterns() -> list[str]:
    return [
        r"\bView more comments\b",
        r"\bSee more comments\b",
        r"\bMore comments\b",
        r"\bView previous comments\b",
        r"\bSee previous comments\b",
        r"\bShow more comments\b",
        r"\u041f\u043e\u043a\u0430\u0437\u0430\u0442\u0438 \u0431\u0456\u043b\u044c\u0448\u0435 \u043a\u043e\u043c\u0435\u043d\u0442\u0430\u0440\u0456\u0432\b",
        r"\u041f\u043e\u043a\u0430\u0437\u0430\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 \u043a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0435\u0432\b",
    ]


def reply_expansion_patterns() -> list[str]:
    return [
        r"\bView replies\b",
        r"\bView more replies\b",
        r"\bSee more replies\b",
        r"\bView previous replies\b",
        r"\bSee previous replies\b",
        r"\bShow replies\b",
        r"\b\d+\s+Replies?\b",
        r"\b\d+\s+Reply\b",
        r"\u0412\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u0456\b",
        r"\u041e\u0442\u0432\u0435\u0442\u044b\b",
    ]


def canonical_post_text(text: str) -> str:
    normalized = normalize_mobile_text(text).lower()
    normalized = normalized.replace("see more", "")
    normalized = re.sub(r"^@\S.*$", "", normalized, flags=re.MULTILINE)
    return re.sub(r"\s+", " ", normalized).strip()


def posts_match(left: PostSnapshot, right: PostSnapshot) -> bool:
    if left.created_at and right.created_at:
        left_dt = datetime.fromisoformat(left.created_at.replace("Z", "+00:00"))
        right_dt = datetime.fromisoformat(right.created_at.replace("Z", "+00:00"))
        if abs((left_dt - right_dt).total_seconds()) > 3600:
            return False
    left_text = canonical_post_text(left.message or "")
    right_text = canonical_post_text(right.message or "")
    if not left_text or not right_text:
        return False
    return (
        left_text[:80] == right_text[:80]
        or left_text.startswith(right_text[:60])
        or right_text.startswith(left_text[:60])
    )


def clean_post_text(raw_text: str, published_hint: str) -> str:
    text = raw_text.replace("\xa0", " ").strip()
    if "\u00c2\u00b7" in text:
        text = text.split("\u00c2\u00b7", 1)[1].strip()
    text = re.sub(r"\bMost relevant\b.*$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    text = re.sub(r"\bAll reactions:.*$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    text = re.sub(r"\bLike\s+Comment\b.*$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    text = re.sub(r"\bLike\s+Comment\s+Share\b.*$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    if published_hint and text.startswith(published_hint):
        text = text[len(published_hint) :].strip()
    return text


def clean_comment_text(raw_text: str, author_name: str, published_hint: str) -> str:
    text = raw_text.replace("\xa0", " ").strip()
    if author_name and text.startswith(author_name):
        text = text[len(author_name) :].lstrip(" \t:,-")
    lines = [normalize_mobile_text(line) for line in text.splitlines() if line.strip()]
    if author_name and lines and lines[0] == author_name:
        lines = lines[1:]
    if published_hint and lines and lines[0].lower() == published_hint.lower():
        lines = lines[1:]
    if published_hint and lines and lines[-1].lower() == published_hint.lower():
        lines = lines[:-1]
    lines = [line for line in lines if not is_comment_control_line(line)]
    while lines and re.fullmatch(r"[\W_]*\d+[\W_]*", lines[-1]):
        lines = lines[:-1]
    while lines and parse_post_timestamp(lines[-1]):
        lines = lines[:-1]
    cleaned = "\n".join(lines).strip()
    if cleaned == published_hint or cleaned == author_name:
        return ""
    return cleaned


def is_comment_control_line(line: str) -> bool:
    normalized = normalize_mobile_text(line).strip(" \t.;:|")
    if not normalized:
        return True
    if re.fullmatch(r"[\W_]+", normalized):
        return True
    if normalized in {"Like", "Reply", "Replies", "Most relevant", "All comments", "Newest"}:
        return True
    if re.fullmatch(r"\d+\s+Repl(?:y|ies)", normalized, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r".{1,80}\s+replied", normalized, flags=re.IGNORECASE):
        return True
    return normalized == "\u00c2\u00b7"


def select_comment_author(raw_author_name: str | None, raw_text: str) -> str | None:
    normalized_author = normalize_mobile_text(raw_author_name or "")
    if is_plausible_comment_author(normalized_author):
        return normalized_author
    return derive_comment_author(raw_text)


def is_plausible_comment_author(value: str) -> bool:
    candidate = normalize_mobile_text(value).strip()
    if not candidate:
        return False
    if len(candidate) > 80 or "\n" in candidate:
        return False
    if parse_post_timestamp(candidate):
        return False
    if re.fullmatch(r"[\W_]*\d+[\W_]*", candidate):
        return False
    if candidate in {
        "Like",
        "Comment",
        "Reply",
        "Most relevant",
        "All comments",
        "View more comments",
    }:
        return False
    return True


def is_meaningful_post_text(text: str) -> bool:
    normalized = (text or "").strip()
    if len(normalized) < 20:
        return False
    noisy_tokens = ("Log In", "Forgot Account?", "See more on Facebook")
    return not any(token in normalized for token in noisy_tokens)


def extract_generic_post_text(body_text: str, meta_description: str, page_name: str) -> str:
    lines = [line.strip() for line in body_text.replace("\xa0", " ").splitlines() if line.strip()]
    collected: list[str] = []
    started = False
    for line in lines:
        if not started:
            if is_ui_line(line, page_name):
                continue
            if len(line) >= 20:
                started = True
                collected.append(line)
            continue
        if is_stop_line(line):
            break
        collected.append(line)
    text = "\n".join(collected).strip()
    if len(text) >= 20:
        return text
    return meta_description.strip()


def derive_comment_author(raw_text: str) -> str | None:
    normalized_text = normalize_mobile_text(raw_text.replace("\xa0", " "))
    lines = [line.strip() for line in normalized_text.splitlines() if line.strip()]
    if lines:
        synthetic_first_line = re.sub(
            r"(?<=[a-z\u0430-\u044f\u0456\u0457\u0454\u0491])(?=[A-Z\u0410-\u042f\u0406\u0407\u0404\u0490])",
            " ",
            lines[0],
        )
        name_tokens: list[str] = []
        for token in re.findall(r"[A-Za-z\u0400-\u04FF'’-]+", synthetic_first_line):
            if not looks_like_name_token(token):
                break
            name_tokens.append(token)
            if len(name_tokens) == 2:
                break
        if len(name_tokens) >= 2:
            candidate = " ".join(name_tokens)
            if is_plausible_comment_author(candidate):
                return candidate
    for line in lines:
        if is_plausible_comment_author(line):
            return line
    return lines[0] if lines else None


def looks_like_name_token(token: str) -> bool:
    candidate = token.strip()
    if not candidate or len(candidate) > 40:
        return False
    if candidate.isupper():
        return True
    return candidate[0].isupper()


def derive_comment_published_hint(raw_text: str) -> str:
    lines = [line.strip() for line in raw_text.replace("\xa0", " ").splitlines() if line.strip()]
    for line in lines[1:]:
        extracted = extract_supported_date_hint_safe(line)
        if extracted:
            return extracted
    return ""


def derive_published_hint_from_body(body_text: str, page_name: str) -> str:
    lines = [line.strip() for line in body_text.replace("\xa0", " ").splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if line == page_name:
            for candidate in lines[index + 1 : index + 5]:
                extracted = extract_supported_date_hint_safe(candidate)
                if extracted:
                    return extracted
    for line in lines:
        extracted = extract_supported_date_hint_safe(line)
        if extracted:
            return extracted
    return ""


def extract_metric_count(raw_text: str) -> int:
    normalized = (raw_text or "").replace(",", "").strip()
    match = re.search(r"(\d+(?:\.\d+)?)\s*([KM]?)", normalized, flags=re.IGNORECASE)
    if not match:
        return 0
    value = float(match.group(1))
    suffix = match.group(2).upper()
    if suffix == "K":
        value *= 1000
    elif suffix == "M":
        value *= 1_000_000
    return int(value)


def is_ui_line(line: str, page_name: str) -> bool:
    normalized = line.strip()
    if not normalized:
        return True
    ui_lines = {
        "Log In",
        "Forgot Account?",
        "More",
        "Home",
        "Live",
        "Reels",
        "Explore",
        "Follow",
        "Comments",
        "Video",
        "Public",
        "Like",
        "Comment",
        "Share",
    }
    if normalized in ui_lines or normalized == page_name:
        return True
    if re.fullmatch(r"\d+:\d+(?:\s*/\s*\d+:\d+)?", normalized):
        return True
    if re.fullmatch(r"\d+(?:\.\d+)?\s*[KM]?(?:\s+views?)?", normalized, flags=re.IGNORECASE):
        return True
    return False


def is_stop_line(line: str) -> bool:
    normalized = line.strip()
    stop_lines = {
        "Like",
        "Comment",
        "Share",
        "Comments",
        "Related Reels",
        "Related Videos",
        "Pages",
        "Privacy",
        "See more on Facebook",
        "Email or phone number",
        "Password",
        "Create new account",
    }
    return normalized in stop_lines


def is_mobile_ui_text(text: str) -> bool:
    return text in {
        "Open app",
        "Log in",
        "Follow",
        "Reels",
        "Photos",
        "Videos",
        "See all",
        "Create new account",
    }


def is_mobile_timeline_end(text: str) -> bool:
    return text.startswith("There's more to see") or text.startswith("See more from ")


def extract_mobile_published_hint(text: str) -> str:
    match = re.search(r"\b\d+\s*(?:m(?:in)?s?|h|d|w)\b", text, flags=re.IGNORECASE)
    return match.group(0) if match else ""


def looks_like_mobile_post_header(text: str, page_name: str) -> bool:
    if not extract_mobile_published_hint(text):
        return False
    first_line = text.splitlines()[0].strip()
    return first_line.startswith(page_name) or first_line == page_name or page_name in first_line


def extract_mobile_post_message(group: list[dict[str, str]], page_name: str) -> str:
    for item in group[1:]:
        text = item["text"]
        if looks_like_mobile_post_header(text, page_name):
            continue
        if re.fullmatch(r"\d+", text):
            continue
        if re.fullmatch(r".+ and \d+ others", text):
            continue
        if len(text) < 20:
            continue
        return re.sub(r"\s+See more$", "", text).strip()
    return ""


def extract_mobile_reactions(group: list[dict[str, str]]) -> int:
    for item in group[1:]:
        match = re.search(r"\band (\d+) others\b", item["text"], flags=re.IGNORECASE)
        if match:
            return int(match.group(1)) + 1
    numeric = [int(item["text"]) for item in group[1:] if re.fullmatch(r"\d+", item["text"])]
    return numeric[0] if numeric else 0


def extract_mobile_comment_count(group: list[dict[str, str]]) -> int:
    saw_reaction_line = False
    numeric_after_reaction: list[int] = []
    for item in group:
        text = item["text"]
        if re.search(r"\band \d+ others\b", text, flags=re.IGNORECASE):
            saw_reaction_line = True
            continue
        if saw_reaction_line and re.fullmatch(r"\d+", text):
            numeric_after_reaction.append(int(text))
    if len(numeric_after_reaction) >= 2:
        return numeric_after_reaction[1]
    return 0


def normalize_permalink(url: str) -> str:
    if not url:
        return ""
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    keep_keys = ("story_fbid", "id", "comment_id", "fbid")
    normalized_query = [(key, query[key]) for key in keep_keys if key in query]
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), urlencode(normalized_query), ""))


def normalize_post_permalink(url: str) -> str:
    if not url:
        return ""
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    keep_keys = ("story_fbid", "id", "fbid")
    normalized_query = [(key, query[key]) for key in keep_keys if key in query]
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), urlencode(normalized_query), ""))


def select_post_permalink(
    *,
    payload_post_permalink: str | None,
    candidate_permalink: str | None,
    detail_url: str,
) -> str:
    generic_paths = {"/reel", "/videos", "/watch"}
    primary = normalize_post_permalink(payload_post_permalink or "")
    if primary and urlsplit(primary).path not in generic_paths:
        return primary
    for fallback in (
        normalize_post_permalink(candidate_permalink or ""),
        normalize_post_permalink(detail_url),
    ):
        if fallback and urlsplit(fallback).path not in generic_paths:
            return fallback
    return primary or normalize_post_permalink(candidate_permalink or "") or normalize_post_permalink(detail_url)


def parse_mobile_timeline_candidates(raw_action_items: list[dict[str, Any]], page_name: str) -> list[dict[str, Any]]:
    items: list[dict[str, str]] = []
    for item in raw_action_items:
        text = normalize_mobile_text(item.get("text") or "")
        if not text or is_mobile_ui_text(text):
            continue
        if items and items[-1]["text"] == text:
            continue
        items.append({"action_id": str(item.get("action_id") or ""), "text": text})

    groups: list[list[dict[str, str]]] = []
    current_group: list[dict[str, str]] = []
    in_posts = False
    for item in items:
        text = item["text"]
        if text == "Posts":
            in_posts = True
            continue
        if not in_posts and looks_like_mobile_post_header(text, page_name):
            in_posts = True
        if not in_posts:
            continue
        if is_mobile_timeline_end(text):
            break
        if looks_like_mobile_post_header(text, page_name):
            if current_group:
                groups.append(current_group)
            current_group = [item]
            continue
        if current_group:
            current_group.append(item)
    if current_group:
        groups.append(current_group)

    candidates: list[dict[str, Any]] = []
    for group in groups:
        header_text = group[0]["text"]
        published_hint = extract_mobile_published_hint(header_text)
        message = extract_mobile_post_message(group, page_name)
        if len(message) < 20:
            continue
        candidates.append(
            {
                "published_hint": published_hint,
                "message": message,
                "reactions": extract_mobile_reactions(group),
                "comments_count": extract_mobile_comment_count(group),
            }
        )
    return candidates
