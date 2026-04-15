from __future__ import annotations

from typing import Any


def validate_source_reference(source: Any) -> None:
    has_reference = any([source.url, source.source_id, source.source_name])
    if not has_reference:
        raise ValueError("At least one of source.url, source.source_id, or source.source_name must be provided.")
    if source.kind == "person_monitor":
        if not source.watchlist and not source.search.enabled:
            raise ValueError("person_monitor source requires source.watchlist or source.search.enabled=true.")
        if source.source_name and not (source.url or source.source_id) and not source.aliases:
            raise ValueError(
                "person_monitor source with only source.source_name requires non-empty source.aliases."
            )
        for index, item in enumerate(source.watchlist):
            if not any([item.url, item.source_id, item.source_name]):
                raise ValueError(
                    f"source.watchlist[{index}] requires url, source_id, or source_name."
                )
        if source.search.enabled and source.search.max_items_per_query <= 0:
            raise ValueError("source.search.max_items_per_query must be greater than zero.")
        return
    if source.platform == "facebook" and not (source.url or source.source_id):
        raise ValueError("Facebook source requires source.url or source.source_id.")


def validate_project_config(config: Any) -> None:
    if not config.sides:
        raise ValueError("At least one side must be configured for stance analysis.")

    if config.source.kind == "person_monitor":
        if config.source.platform == "facebook":
            if config.collector.mode not in {"web", "hybrid"}:
                raise ValueError("Facebook person_monitor requires collector.mode='web' or collector.mode='hybrid'.")
            if not config.collector.public_web.enabled:
                raise ValueError("Facebook person_monitor requires collector.public_web.enabled=true.")
            return
        if config.source.platform == "telegram":
            if config.collector.mode not in {"mtproto", "web"}:
                raise ValueError("Telegram person_monitor requires collector.mode='mtproto' or collector.mode='web'.")
        elif config.source.platform == "x":
            if config.collector.mode not in {"x_api", "web"}:
                raise ValueError("X person_monitor requires collector.mode='x_api' or collector.mode='web'.")
        elif config.source.platform == "threads":
            if config.collector.mode not in {"threads_api", "web"}:
                raise ValueError("Threads person_monitor requires collector.mode='threads_api' or collector.mode='web'.")
        elif config.source.platform == "instagram":
            if config.collector.mode not in {"instagram_graph_api", "web"}:
                raise ValueError(
                    "Instagram person_monitor requires collector.mode='instagram_graph_api' or collector.mode='web'."
                )

    if config.source.platform == "telegram":
        if config.collector.mode not in {"mtproto", "web", "bot_api"}:
            raise ValueError("Telegram source requires collector.mode='mtproto', collector.mode='web', or collector.mode='bot_api'.")
        if config.collector.mode == "mtproto":
            if not config.collector.telegram_mtproto.enabled:
                raise ValueError("Telegram source requires collector.telegram_mtproto.enabled=true.")
            telegram_config = config.collector.telegram_mtproto
            missing_fields: list[str] = []
            if not telegram_config.session_file:
                missing_fields.append("collector.telegram_mtproto.session_file or TELEGRAM_SESSION_FILE")
            if telegram_config.api_id is None:
                missing_fields.append("collector.telegram_mtproto.api_id or TELEGRAM_API_ID")
            if not telegram_config.api_hash:
                missing_fields.append("collector.telegram_mtproto.api_hash or TELEGRAM_API_HASH")
            if missing_fields:
                raise ValueError("Telegram source requires " + ", ".join(missing_fields) + ".")
        elif config.collector.mode == "bot_api":
            if not config.collector.telegram_bot_api.enabled:
                raise ValueError("Telegram Bot API source requires collector.telegram_bot_api.enabled=true.")
            if not config.collector.telegram_bot_api.bot_token:
                raise ValueError("Telegram Bot API source requires collector.telegram_bot_api.bot_token or TELEGRAM_BOT_TOKEN.")
        else:
            if not config.collector.telegram_web.enabled:
                raise ValueError("Telegram web source requires collector.telegram_web.enabled=true.")
        return

    if config.source.platform == "x":
        if config.collector.mode not in {"x_api", "web"}:
            raise ValueError("X source requires collector.mode='x_api' or collector.mode='web'.")
        if config.collector.mode == "x_api":
            if not config.collector.x_api.enabled:
                raise ValueError("X source requires collector.x_api.enabled=true.")
            if not config.collector.x_api.bearer_token:
                raise ValueError("X source requires collector.x_api.bearer_token or X_BEARER_TOKEN.")
        else:
            if not config.collector.x_web.enabled:
                raise ValueError("X web source requires collector.x_web.enabled=true.")
        return

    if config.source.platform == "threads":
        if config.collector.mode not in {"threads_api", "web"}:
            raise ValueError("Threads source requires collector.mode='threads_api' or collector.mode='web'.")
        if config.collector.mode == "threads_api":
            if not config.collector.threads_api.enabled:
                raise ValueError("Threads source requires collector.threads_api.enabled=true.")
            if not config.collector.threads_api.access_token:
                raise ValueError("Threads source requires collector.threads_api.access_token or THREADS_ACCESS_TOKEN.")
        else:
            if not config.collector.threads_web.enabled:
                raise ValueError("Threads web source requires collector.threads_web.enabled=true.")
        return

    if config.source.platform == "instagram":
        if config.collector.mode not in {"instagram_graph_api", "web"}:
            raise ValueError("Instagram source requires collector.mode='instagram_graph_api' or collector.mode='web'.")
        if config.collector.mode == "instagram_graph_api":
            if not config.collector.instagram_graph_api.enabled:
                raise ValueError("Instagram source requires collector.instagram_graph_api.enabled=true.")
            if not config.collector.instagram_graph_api.access_token:
                raise ValueError(
                    "Instagram source requires collector.instagram_graph_api.access_token or INSTAGRAM_ACCESS_TOKEN."
                )
            if not config.source.source_id:
                raise ValueError("Instagram Graph API source requires source.source_id.")
        else:
            if not config.collector.instagram_web.enabled:
                raise ValueError("Instagram web source requires collector.instagram_web.enabled=true.")
        return

    if config.collector.mode == "mtproto":
        raise ValueError("Facebook source cannot use collector.mode='mtproto'.")
    if config.collector.mode == "bot_api":
        raise ValueError("Facebook source cannot use collector.mode='bot_api'.")
    if config.collector.mode == "x_api":
        raise ValueError("Facebook source cannot use collector.mode='x_api'.")
    if config.collector.mode == "threads_api":
        raise ValueError("Facebook source cannot use collector.mode='threads_api'.")
    if config.collector.mode == "instagram_graph_api":
        raise ValueError("Facebook source cannot use collector.mode='instagram_graph_api'.")
    if config.collector.mode == "api" and not config.collector.meta_api.enabled:
        raise ValueError("collector.meta_api.enabled must be true when collector.mode='api'.")
    if config.collector.mode == "web" and not config.collector.public_web.enabled:
        raise ValueError("collector.public_web.enabled must be true when collector.mode='web'.")
    if config.collector.mode == "hybrid" and not (
        config.collector.meta_api.enabled or config.collector.public_web.enabled
    ):
        raise ValueError("collector.hybrid requires at least one enabled Facebook collector.")
