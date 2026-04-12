from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .facebook_web_content import extract_metric_count, normalize_post_permalink
from .facebook_web_timestamps import extract_numeric_media_id


def extract_feed_candidates(page: Any) -> list[dict[str, Any]]:
    script = """
    () => {
      const articles = Array.from(document.querySelectorAll('div[role="article"], article'));
      return articles.map((article) => {
        const statusLinks = Array.from(article.querySelectorAll('a[href*="/posts/"], a[href*="/permalink/"], a[href*="story_fbid="], a[href*="/videos/"], a[href*="/reel/"]'))
          .map((node) => node.href || '')
          .filter(Boolean)
          .filter((value, index, rows) => rows.indexOf(value) === index);
        const permalinkNode = article.querySelector('a[href*="/posts/"], a[href*="/permalink/"], a[href*="story_fbid="], a[href*="/videos/"], a[href*="/reel/"]');
        return {
          permalink: statusLinks[0] || permalinkNode?.href || null,
          shared_permalink: statusLinks.find((href) => href !== (statusLinks[0] || permalinkNode?.href || '')) || null,
          published_hint: (permalinkNode?.innerText || '').trim(),
          text: (article.innerText || '').trim(),
          reactions_text: '',
          comments_text: '',
          shares_text: ''
        };
      }).filter(item => item.permalink && item.text);
    }
    """
    return page.evaluate(script)


def extract_plugin_feed_candidates(page: Any) -> list[dict[str, Any]]:
    script = """
    () => {
      const wrappers = Array.from(document.querySelectorAll('div._5pcr.userContentWrapper'));
      const metricValue = (wrapper, title) => {
        const metricNodes = Array.from(wrapper.querySelectorAll('.embeddedLikeButton, a._29bd div, [title]'));
        const match = metricNodes.find((node) => {
          const value = (node.getAttribute('title') || node.innerText || '').trim().toLowerCase();
          return value.includes(title);
        });
        return (match?.innerText || '').trim();
      };
      return wrappers.map((wrapper) => {
        const timestampNode = wrapper.querySelector('abbr[data-utime]');
        const statusLinks = Array.from(wrapper.querySelectorAll('a[href*="/posts/"], a[href*="/permalink/"], a[href*="story_fbid="], a[href*="/videos/"], a[href*="/reel/"]'))
          .map((node) => node.href || '')
          .filter(Boolean)
          .filter((value, index, rows) => rows.indexOf(value) === index);
        const timestampLink =
          timestampNode?.closest('a[href*="/posts/"], a[href*="/permalink/"], a[href*="story_fbid="], a[href*="/videos/"], a[href*="/reel/"]') ||
          wrapper.querySelector('a[href*="/posts/"], a[href*="/permalink/"], a[href*="story_fbid="], a[href*="/videos/"], a[href*="/reel/"]');
        const messageNodes = Array.from(wrapper.querySelectorAll('[data-testid="post_message"]'));
        const primaryMessage = (messageNodes[0]?.innerText || '').trim();
        const allMessages = messageNodes
          .map((node) => (node.innerText || '').trim())
          .filter(Boolean)
          .join('\\n\\n');
        return {
          permalink: statusLinks[0] || timestampLink?.href || null,
          shared_permalink: statusLinks.find((href) => href !== (statusLinks[0] || timestampLink?.href || '')) || null,
          published_hint: (timestampNode?.innerText || '').trim(),
          published_at: timestampNode?.dataset?.utime ? new Date(Number(timestampNode.dataset.utime) * 1000).toISOString() : null,
          text: primaryMessage || allMessages,
          author_name: (wrapper.querySelector('._50f7, .fwb a, .fwb span')?.innerText || '').trim() || null,
          reactions_text: metricValue(wrapper, 'like'),
          comments_text: metricValue(wrapper, 'comment'),
          shares_text: metricValue(wrapper, 'share'),
        };
      }).filter(item => item.permalink && item.text);
    }
    """
    return page.evaluate(script)


def extract_video_candidates(page: Any) -> list[dict[str, Any]]:
    script = """
    () => {
      const links = Array.from(document.querySelectorAll('a[href*="/videos/"]'));
      return links.map((link) => ({
        permalink: link.href,
        detail_url: link.href,
        published_hint: '',
        published_at: null,
        text: (link.innerText || '').trim(),
        author_name: null,
        reactions_text: '',
        comments_text: '',
        shares_text: '',
      }));
    }
    """
    return page.evaluate(script)


def extract_photo_candidates(page: Any) -> list[dict[str, Any]]:
    script = """
    () => {
      const links = Array.from(document.querySelectorAll('a[href*="photo.php?fbid="]'));
      return links.map((link) => ({
        permalink: link.href,
        detail_url: link.href,
        published_hint: '',
        published_at: null,
        text: '',
        author_name: null,
        reactions_text: '',
        comments_text: '',
        shares_text: '',
      }));
    }
    """
    return page.evaluate(script)


def extract_reel_candidates(page: Any) -> list[dict[str, Any]]:
    script = """
    () => {
      const links = Array.from(document.querySelectorAll('a[href*="/reel/"]'));
      return links.map((link) => ({
        permalink: link.href,
        detail_url: link.href,
        published_hint: '',
        published_at: null,
        text: (link.innerText || '').trim(),
        author_name: null,
        reactions_text: '',
        comments_text: '',
        shares_text: '',
      }));
    }
    """
    return page.evaluate(script)


def extract_post_page(page: Any, *, comment_limit: int = 200) -> dict[str, Any]:
    script = """
    (commentLimit) => {
      const isVisible = (element) => {
        if (!element) {
          return false;
        }
        const rect = element.getBoundingClientRect();
        const style = window.getComputedStyle(element);
        return (
          rect.width > 0 &&
          rect.height > 0 &&
          style.display !== 'none' &&
          style.visibility !== 'hidden'
        );
      };
      const normalizeText = (value) => (value || '').replace(/\\u00a0/g, ' ').trim();
      const looksLikeAuthor = (value) => {
        const normalized = normalizeText(value);
        return (
          normalized &&
          normalized.length <= 80 &&
          !/\\b(?:comment|reply|replies|like|share)\\b/i.test(normalized) &&
          !/\\b\\d+\\s*(?:m(?:in)?s?|h|d|w)\\b/i.test(normalized)
        );
      };
      const extractCommentFromArticle = (article) => {
        const links = Array.from(article.querySelectorAll('a[href], a[role="link"]'));
        const authorLink = links.find((link) => looksLikeAuthor(link.innerText || '')) || null;
        const commentPermalink = links.find((link) => (link.href || '').includes('comment_id=')) || null;
        const rect = article.getBoundingClientRect();
        return {
          text: normalizeText(article.innerText || ''),
          author_name: normalizeText(authorLink?.innerText || '') || null,
          permalink: commentPermalink?.href || null,
          published_hint: normalizeText(commentPermalink?.innerText || ''),
          nesting_x: Math.round(rect.x),
        };
      };
      const findFallbackCommentBlock = (link) => {
        const candidates = [];
        let current = link.parentElement;
        for (let depth = 0; current && depth < 8; depth += 1, current = current.parentElement) {
          if (!isVisible(current)) {
            continue;
          }
          const text = normalizeText(current.innerText || '');
          if (!text || text.length < 15 || text.length > 2000) {
            continue;
          }
          const commentLinks = Array.from(current.querySelectorAll('a[href*="comment_id="]')).filter(isVisible).length;
          const score =
            (commentLinks === 1 ? 1000 : Math.max(0, 200 - commentLinks * 50)) +
            Math.max(0, 800 - text.length) -
            depth * 25;
          candidates.push({ element: current, score });
        }
        candidates.sort((left, right) => right.score - left.score);
        return candidates[0]?.element || null;
      };
      const extractFallbackComments = () => {
        if (!window.location.href.includes('/reel/')) {
          return [];
        }
        const seen = new Set();
        const commentLinks = Array.from(document.querySelectorAll('a[href*="comment_id="]'))
          .filter(isVisible)
          .filter((link) => {
            const href = link.href || '';
            if (!href || seen.has(href)) {
              return false;
            }
            seen.add(href);
            return true;
          });
        return commentLinks
          .map((link) => {
            const block = findFallbackCommentBlock(link);
            if (!block) {
              return null;
            }
            const blockLinks = Array.from(block.querySelectorAll('a[href], a[role="link"]')).filter(isVisible);
            const authorLink = blockLinks.find((candidate) => looksLikeAuthor(candidate.innerText || '')) || null;
            const rect = block.getBoundingClientRect();
            return {
              dom_key: link.href || null,
              dom_parent_key: null,
              text: normalizeText(block.innerText || ''),
              author_name: normalizeText(authorLink?.innerText || '') || null,
              permalink: link.href || null,
              published_hint: normalizeText(link.innerText || ''),
              nesting_x: Math.round(rect.x),
            };
          })
          .filter((comment) => comment && comment.text && (comment.author_name || comment.permalink || comment.published_hint))
          .slice(0, commentLimit * 2);
      };
      const articles = Array.from(document.querySelectorAll('div[role="article"], article'));
      const firstArticle = articles[0] || null;
      const timestampNode = firstArticle?.querySelector('abbr[data-utime], span.timestampContent');
      const statusLinks = Array.from(
        firstArticle?.querySelectorAll('a[href*="/posts/"]:not([href*="comment_id="]), a[href*="/videos/"], a[href*="/reel/"], a[href*="story_fbid="]') || []
      )
        .map((node) => node.href || '')
        .filter(Boolean)
        .filter((value, index, rows) => rows.indexOf(value) === index);
      const permalinkNode =
        firstArticle?.querySelector('a[href*="/posts/"]:not([href*="comment_id="]), a[href*="/videos/"], a[href*="/reel/"], a[href*="story_fbid="]') ||
        document.querySelector('a[href*="/posts/"]:not([href*="comment_id="]), a[href*="/videos/"], a[href*="/reel/"], a[href*="story_fbid="]');
      const getMeta = (property) => document.querySelector(`meta[property="${property}"]`)?.content || null;
      const comments = articles
        .slice(1)
        .map(extractCommentFromArticle)
        .filter((comment) => {
          const text = (comment.text || '').trim();
          return text && (comment.author_name || comment.permalink || comment.published_hint);
        })
        .slice(0, commentLimit);
      const reelFallbackComments = extractFallbackComments();
      return {
        post_text: firstArticle?.innerText || '',
        post_permalink: statusLinks[0] || permalinkNode?.href || getMeta('og:url') || window.location.href,
        shared_permalink: statusLinks.find((href) => href !== (statusLinks[0] || permalinkNode?.href || '')) || null,
        published_hint: (timestampNode?.innerText || permalinkNode?.innerText || '').trim(),
        published_at: timestampNode?.dataset?.utime ? new Date(Number(timestampNode.dataset.utime) * 1000).toISOString() : null,
        body_text: (document.body?.innerText || '').trim(),
        meta_title: getMeta('og:title'),
        meta_description: getMeta('og:description'),
        comments,
        reel_fallback_comments: reelFallbackComments,
      };
    }
    """
    return page.evaluate(script, comment_limit)


def extract_mobile_timeline_payload(page: Any) -> dict[str, Any]:
    script = """
    () => ({
      url: window.location.href,
      body_text: (document.body?.innerText || '').trim(),
      action_items: Array.from(document.querySelectorAll('[data-action-id]'))
        .map((node) => ({
          action_id: node.getAttribute('data-action-id'),
          text: (node.innerText || '').trim(),
        }))
        .filter((item) => item.text),
    })
    """
    return page.evaluate(script)


def postprocess_candidates(raw_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    processed: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        permalink = normalize_post_permalink(candidate.get("permalink") or "")
        if not permalink or permalink in seen:
            continue
        seen.add(permalink)
        processed.append(
            {
                "permalink": permalink,
                "detail_url": candidate.get("detail_url") or permalink,
                "published_hint": (candidate.get("published_hint") or "").strip(),
                "published_at": candidate.get("published_at"),
                "text": (candidate.get("text") or "").strip(),
                "author_name": candidate.get("author_name"),
                "shared_permalink": candidate.get("shared_permalink"),
                "reactions": extract_metric_count(candidate.get("reactions_text") or ""),
                "comments_count": extract_metric_count(candidate.get("comments_text") or ""),
                "shares": extract_metric_count(candidate.get("shares_text") or ""),
            }
        )
    return processed


def propagation_metadata(
    *,
    payload: dict[str, Any],
    post_text: str,
    post_permalink: str | None,
) -> tuple[str | None, str | None, str | None, str | None]:
    joined_text = " ".join(
        part
        for part in [
            post_text,
            str(payload.get("body_text") or ""),
            str(payload.get("meta_title") or ""),
            str(payload.get("meta_description") or ""),
            str(payload.get("reply_text") or ""),
        ]
        if part
    ).lower()
    if "shared a post" not in joined_text and "shared a memory" not in joined_text and "shared" not in joined_text:
        return None, None, None, None
    origin_permalink = (
        payload.get("shared_permalink")
        or payload.get("origin_permalink")
        or payload.get("url")
        or post_permalink
    )
    origin_external_id = extract_numeric_media_id(origin_permalink or "") or None
    origin_post_id = f"facebook:origin:{origin_external_id}" if origin_external_id else None
    return "share", origin_post_id, origin_external_id, origin_permalink


def page_plugin_url(page_url: str) -> str:
    return urlunsplit(
        (
            "https",
            "www.facebook.com",
            "/plugins/page.php",
            urlencode(
                {
                    "href": page_url,
                    "tabs": "timeline",
                    "width": "500",
                    "height": "5000",
                    "small_header": "false",
                    "adapt_container_width": "true",
                    "hide_cover": "false",
                    "show_facepile": "false",
                    "locale": "en_US",
                }
            ),
            "",
        )
    )


def page_tab_url(page_url: str, tab_name: str) -> str:
    normalized_page_url = page_url.rstrip("/")
    return with_locale(f"{normalized_page_url}/{tab_name}")


def mobile_page_url(page_url: str) -> str:
    parts = urlsplit(page_url)
    return urlunsplit(("https", "m.facebook.com", parts.path.rstrip("/"), "", ""))


def with_locale(url: str) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["locale"] = "en_US"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
