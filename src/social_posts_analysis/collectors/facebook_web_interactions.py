from __future__ import annotations

import time
from typing import Any

from .facebook_web_content import (
    comment_article_limit,
    comment_expansion_patterns,
    comment_sort_menu_patterns,
    comment_sort_option_patterns,
    reel_comment_entry_patterns,
    reply_expansion_patterns,
)


def prepare_post_detail_page(
    page: Any,
    *,
    target_comment_count: int = 0,
    aggressive: bool = False,
    max_seconds: float | None = None,
) -> None:
    accept_desktop_cookies(page)
    if target_comment_count <= 0 and not aggressive:
        return
    if _is_reel_page(page):
        click_buttonish_text(
            page,
            patterns=reel_comment_entry_patterns(),
            max_clicks=2 if aggressive else 1,
            wait_ms=1200,
        )
    click_buttonish_text(
        page,
        patterns=[r"\b\d+(?:\.\d+)?\s*[KM]?\s+comments?\b"],
        max_clicks=2 if aggressive else 1,
        wait_ms=1500,
    )
    if aggressive and target_comment_count > 0:
        click_buttonish_text(
            page,
            patterns=[r"\bComment\b"],
            max_clicks=1,
            wait_ms=1000,
        )
    click_buttonish_text(
        page,
        patterns=comment_sort_menu_patterns(),
        max_clicks=1,
        wait_ms=1000,
    )
    click_buttonish_text(
        page,
        patterns=comment_sort_option_patterns(aggressive=aggressive),
        max_clicks=2 if aggressive else 1,
        wait_ms=2500,
    )
    expand_comment_threads(
        page,
        target_comment_count=target_comment_count,
        aggressive=aggressive,
        max_seconds=max_seconds,
    )


def expand_comment_threads(
    page: Any,
    *,
    target_comment_count: int = 0,
    aggressive: bool = False,
    max_seconds: float | None = None,
) -> None:
    deadline = time.monotonic() + max_seconds if max_seconds is not None else None
    last_article_count = count_article_nodes(page)
    stable_rounds = 0
    max_rounds = 16 + min(target_comment_count // 20, 10)
    if aggressive:
        max_rounds += 12
    for _ in range(max_rounds):
        if deadline is not None and time.monotonic() >= deadline:
            break
        page.mouse.wheel(0, 1800)
        scrolled = scroll_primary_comment_container(page)
        page.wait_for_timeout(1200)
        more_clicked = click_buttonish_text(
            page,
            patterns=comment_expansion_patterns(),
            max_clicks=6 if aggressive else 3,
            wait_ms=1200,
        )
        reply_clicked = click_buttonish_text(
            page,
            patterns=reply_expansion_patterns(),
            max_clicks=18 if aggressive else 10,
            wait_ms=900,
        )
        if reply_clicked:
            scroll_primary_comment_container(page)
            page.wait_for_timeout(1000)

        if aggressive and last_article_count <= 1 and target_comment_count > 0:
            click_buttonish_text(
                page,
                patterns=[r"\b\d+(?:\.\d+)?\s*[KM]?\s+comments?\b", r"\bComment\b"],
                max_clicks=2,
                wait_ms=1000,
            )
            if _is_reel_page(page):
                click_buttonish_text(
                    page,
                    patterns=reel_comment_entry_patterns(),
                    max_clicks=2,
                    wait_ms=900,
                )

        article_count = count_article_nodes(page)
        if article_count <= last_article_count and not scrolled and more_clicked == 0 and reply_clicked == 0:
            stable_rounds += 1
        else:
            stable_rounds = 0
        last_article_count = max(last_article_count, article_count)
        enough_comments = (
            article_count >= min(max(target_comment_count, 0) + 1, comment_article_limit(target_comment_count, aggressive))
            if target_comment_count
            else False
        )
        if enough_comments or stable_rounds >= (3 if aggressive else 2):
            break


def accept_desktop_cookies(page: Any) -> None:
    for label in ("Decline optional cookies", "Only allow essential cookies", "Allow all cookies"):
        try:
            button = page.get_by_text(label, exact=False)
            if button.count():
                button.first.click(timeout=5000, force=True)
                page.wait_for_timeout(2000)
                return
        except Exception:
            continue


def click_buttonish_text(
    page: Any,
    *,
    patterns: list[str],
    max_clicks: int,
    wait_ms: int,
) -> int:
    clicked = 0
    for _ in range(max_clicks):
        clicked_text = page.evaluate(
            """
            (patterns) => {
              const isVisible = (element) => {
                const rect = element.getBoundingClientRect();
                const style = window.getComputedStyle(element);
                return (
                  rect.width > 0 &&
                  rect.height > 0 &&
                  style.display !== 'none' &&
                  style.visibility !== 'hidden'
                );
              };
              const candidates = Array.from(
                document.querySelectorAll('div[role="button"], a[role="button"], span[role="button"], button')
              );
              for (const pattern of patterns) {
                const regex = new RegExp(pattern, 'i');
                const target = candidates.find((element) => {
                  if (!isVisible(element)) {
                    return false;
                  }
                  const value = `${element.innerText || ''} ${element.getAttribute('aria-label') || ''}`.trim();
                  return regex.test(value);
                });
                if (target) {
                  target.scrollIntoView({ block: 'center', inline: 'nearest' });
                  target.click();
                  return `${target.innerText || ''} ${target.getAttribute('aria-label') || ''}`.trim();
                }
              }
              return null;
            }
            """,
            patterns,
        )
        if not clicked_text:
            break
        clicked += 1
        page.wait_for_timeout(wait_ms)
    return clicked


def scroll_primary_comment_container(page: Any) -> bool:
    return bool(
        page.evaluate(
            """
            () => {
              const root = document.querySelector('[role="dialog"]') || document;
              const candidates = Array.from(root.querySelectorAll('div'))
                .map((element) => {
                  const style = window.getComputedStyle(element);
                  const text = (element.innerText || '').trim();
                  const overflowY = style.overflowY;
                  if (!['auto', 'scroll'].includes(overflowY)) {
                    return null;
                  }
                  if (element.scrollHeight <= element.clientHeight + 40 || element.clientHeight < 300) {
                    return null;
                  }
                  const score = (element.scrollHeight - element.clientHeight) + (text.includes('comments') ? 100000 : 0);
                  return { element, score };
                })
                .filter(Boolean)
                .sort((left, right) => right.score - left.score);
              const target = candidates[0]?.element;
              if (!target) {
                return false;
              }
              const before = target.scrollTop;
              target.scrollTop = target.scrollHeight;
              return target.scrollTop > before + 20;
            }
            """
        )
    )


def count_article_nodes(page: Any) -> int:
    return page.locator('div[role="article"], article').count()


def accept_mobile_cookies(page: Any) -> None:
    for label in ("Only allow essential cookies", "Decline optional cookies"):
        try:
            button = page.get_by_text(label, exact=False)
            if button.count():
                button.first.click(timeout=5000, force=True)
                page.wait_for_timeout(2500)
                return
        except Exception:
            continue


def _is_reel_page(page: Any) -> bool:
    return "/reel/" in str(getattr(page, "url", "") or "")
